from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, split, trim, lower, current_timestamp, monotonically_increasing_id, min as spark_min, array, explode
from pyspark.sql.types import StringType, LongType, StructType, StructField
from pyspark.sql.functions import udf
import os
import random
from datetime import datetime, timedelta

# ✳️ Giả lập nội dung bài đăng (post)
sample_reviews = [
    "Sản phẩm này đã thay đổi cuộc đời tôi!", "Rất khuyến khích!", "Giá trị tuyệt vời!",
    "Sẽ mua lại.", "Không như mong đợi.", "Ý tưởng quà tặng hoàn hảo!"
]

# ⚙️ Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("NormalizeSocialMedia") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Đường dẫn thư mục chứa các file dữ liệu
base_path = "/opt/airflow/files"

# Liệt kê tất cả các file có đuôi .csv trong thư mục
platform_files = [f for f in os.listdir(base_path) if f.endswith('.csv')]

# Khởi tạo các DataFrame tổng hợp và bộ đếm ID
users_df_all, posts_df_all, hashtags_df_all, post_hashtag_df_all, user_mentions_df_all = None, None, None, None, spark.createDataFrame([], StructType([
    StructField("post_id", StringType(), True),
    StructField("mentioned_user_id", LongType(), True)
])) # Khởi tạo user_mentions_df_all với schema dự kiến

user_id_counter = 0
hashtag_id_counter = 0
user_map = {}  # Dictionary ánh xạ display_name với user_id
hashtag_map = {} # Dictionary ánh xạ hashtag_text với hashtag_id

# UDF (User Defined Function) để sinh nội dung ngẫu nhiên
random_review_udf = udf(lambda: random.choice(sample_reviews), StringType())
# UDF để sinh số nguyên ngẫu nhiên
random_int_udf = udf(lambda: random.randint(100, 100000), LongType())

# Dictionary ánh xạ tên cột cũ sang tên cột mới (chuẩn hóa)
rename_map = {
    "user_url": "user_posted",
    "post_user": "user_posted",
    "commenter_url": "user_posted",
    "date_created": "comment_date",
    "post_date_created": "comment_date",
    "comment": "comment_text",         # nếu bạn muốn tái sử dụng cho hashtag/mentions
    "comment_user": "user_posted",      # Instagram
    "commenter_user_name": "display_name" # TikTok
}

# Đọc và xử lý từng file dữ liệu
for filename in platform_files:
    platform = filename.split("-")[0]  # Giả sử tên platform có dạng "Facebook-datasets.csv"
    df = spark.read.option("header", True).csv(os.path.join(base_path, filename))

    # Chuẩn hóa tên cột
    for src, dst in rename_map.items():
        if src in df.columns and dst not in df.columns:
            df = df.withColumnRenamed(src, dst)

    df_cols = df.columns

    # Xử lý bảng USERS (người dùng)
    if "user_posted" in df_cols:
        # Lấy danh sách duy nhất các user_posted (có thể là URL)
        user_urls = df.select("user_posted").dropna().distinct().withColumnRenamed("user_posted", "user_url")

        # Tạo user_id và các thông tin khác cho người dùng
        users_df = user_urls.withColumn("user_id", monotonically_increasing_id() + user_id_counter) \
            .withColumn("display_name", split(col("user_url"), "/").getItem(-1)) \
            .withColumn("bio", lit(f"Người dùng từ {platform}")) \
            .withColumn("is_verified", lit(True)) \
            .withColumn("followers_count", random_int_udf()) \
            .withColumn("following_count", random_int_udf()) \
            .withColumn("post_count", random_int_udf()) \
            .withColumn("type_of_account", lit(platform)) \
            .select("user_id", "display_name", "bio", "is_verified", "followers_count", "following_count", "post_count", "type_of_account") \
            .dropDuplicates(["display_name"]) # Loại bỏ các dòng trùng lặp dựa trên display_name

        # Cập nhật bộ đếm user_id
        user_id_counter += 100000
        # Tạo dictionary ánh xạ display_name với user_id mới và cập nhật vào user_map chung
        new_user_map = users_df.select("display_name", "user_id").rdd.collectAsMap()
        user_map.update(new_user_map)

        # Thêm DataFrame người dùng hiện tại vào DataFrame tổng hợp
        users_df_all = users_df if users_df_all is None else users_df_all.union(users_df)

    # Xử lý bảng POSTS (bài đăng)
    if "post_url" in df_cols and "user_posted" in df_cols:
        # Hàm UDF để trích xuất post_id từ các cột URL khác nhau
        def extract_post_id(url, post_id, post_url):
            if post_id:  # Ưu tiên cột post_id nếu có
                return post_id
            elif url and not post_url:  # Nếu có URL khác và không phải post_url
                return url.rstrip("/").split("/")[-1]
            elif post_url:  # Nếu có cột post_url
                return post_url.rstrip("/").split("/")[-1]
            else:  # Trả về None nếu không tìm thấy
                return None

        extract_post_id_udf = udf(extract_post_id, StringType())
        # UDF để lấy user_id từ user_posted (URL) dựa trên user_map, xử lý trường hợp None
        get_user_id_udf = udf(lambda url: user_map.get(url.split("/")[-1]) if url else None, LongType())

        # Lấy ngày comment sớm nhất và trừ đi 1 ngày để làm ngày đăng bài (giả định)
        if "comment_date" in df_cols:
            try:
                min_date_row = df.select(spark_min("comment_date").alias("min_date")).collect()[0]
                min_date_str = min_date_row["min_date"]
                min_date = datetime.strptime(min_date_str, "%Y-%m-%d") - timedelta(days=1)
            except:
                min_date = datetime.now() - timedelta(days=1)
        else:
            min_date = datetime.now() - timedelta(days=1)

        # Tạo DataFrame posts_df
        posts_df = df.withColumn("post_id", extract_post_id_udf(col("post_url"), col("post_id"), col("url"))) \
            .withColumn("user_id", get_user_id_udf(col("user_posted"))) \
            .withColumn("post_content", random_review_udf()) \
            .withColumn("post_date", lit(min_date.strftime("%Y-%m-%d"))) \
            .withColumn("post_url", col("post_url")) \
            .select("post_id", "user_id", "post_content", "post_date", "post_url") \
            .dropDuplicates(["post_id"]) # Loại bỏ các bài đăng trùng lặp

        # Thêm DataFrame bài đăng hiện tại vào DataFrame tổng hợp
        posts_df_all = posts_df if posts_df_all is None else posts_df_all.union(posts_df)

    # Xử lý bảng HASHTAGS
    if "hashtag_comment" in df_cols:
        # Lấy danh sách duy nhất các hashtag
        hashtags_df = df.select("hashtag_comment").dropna().distinct() \
            .withColumn("hashtag_text", trim(lower(col("hashtag_comment")))) \
            .drop("hashtag_comment") \
            .withColumn("hashtag_id", monotonically_increasing_id() + hashtag_id_counter) \
            .dropDuplicates(["hashtag_text"]) # Loại bỏ hashtag trùng lặp

        # Cập nhật bộ đếm hashtag_id
        hashtag_id_counter += 100000
        # Tạo dictionary ánh xạ hashtag_text với hashtag_id mới và cập nhật vào hashtag_map chung
        new_hashtag_map = hashtags_df.select("hashtag_text", "hashtag_id").rdd.collectAsMap()
        hashtag_map.update(new_hashtag_map)

        # Thêm DataFrame hashtag hiện tại vào DataFrame tổng hợp
        hashtags_df_all = hashtags_df if hashtags_df_all is None else hashtags_df_all.union(hashtags_df)

    # Xử lý bảng FACT_POST_HASHTAGS (liên kết bài đăng và hashtag)
    if "post_url" in df_cols and "hashtag_comment" in df_cols:
        # UDF để lấy hashtag_id từ hashtag_text dựa trên hashtag_map
        get_hashtag_id_udf = udf(lambda text: hashtag_map.get(text), LongType())
        post_hashtag_df = df.select("post_url", "hashtag_comment").dropna() \
            .withColumn("post_id", split(col("post_url"), "/").getItem(-1)) \
            .withColumn("hashtag_text", trim(lower(col("hashtag_comment")))) \
            .withColumn("hashtag_id", get_hashtag_id_udf(col("hashtag_text"))) \
            .select("post_id", "hashtag_id") \
            .dropDuplicates() # Loại bỏ các cặp post_id và hashtag_id trùng lặp

        # Thêm DataFrame liên kết bài đăng-hashtag hiện tại vào DataFrame tổng hợp
        post_hashtag_df_all = post_hashtag_df if post_hashtag_df_all is None else post_hashtag_df_all.union(post_hashtag_df)

    # Xử lý bảng FACT_USER_MENTIONS (người dùng được nhắc đến)
    has_mention_data = False
    if "tagged_user_in" in df_cols and df.filter(col("tagged_user_in").isNotNull()).count() > 0 and "post_url" in df_cols:
        has_mention_data = True
    elif "tagged_users" in df_cols and df.filter(col("tagged_users").isNotNull()).count() > 0 and "post_url" in df_cols:
        has_mention_data = True

    if has_mention_data and "post_url" in df_cols:
        # UDF để trích xuất user_id của người được nhắc đến từ URL, xử lý trường hợp None
        def extract_mention_id(url):
            if url is None:
                return None
            username = url.strip().split("/")[-1]
            return user_map.get(username)

        get_mentioned_id_udf = udf(extract_mention_id, LongType())

        # Chuẩn bị cột chứa thông tin người được tag
        mention_cols = []
        if "tagged_user_in" in df_cols:
            mention_cols.append(col("tagged_user_in"))
        if "tagged_users" in df_cols:
            mention_cols.append(col("tagged_users"))

        # Gộp các cột mention lại thành một mảng, sau đó tách (explode) thành nhiều dòng
        mentions_df = df.filter(col("post_url").isNotNull()) \
            .withColumn("mention", explode(array(*mention_cols))) \
            .filter(col("mention").isNotNull()) \
            .withColumn("post_id", split(col("post_url"), "/").getItem(-1)) \
            .withColumn("mentioned_user_id", get_mentioned_id_udf(col("mention"))) \
            .select("post_id", "mentioned_user_id") \
            .dropDuplicates() # Loại bỏ các cặp post_id và mentioned_user_id trùng lặp

        # Thêm DataFrame mentions hiện tại vào DataFrame tổng hợp
        user_mentions_df_all = user_mentions_df_all.union(mentions_df)

# 🟫 Ghi các DataFrame tổng hợp xuống Bronze layer (lớp dữ liệu thô đã được chuẩn hóa)
if users_df_all:
    users_df_all.withColumn("ingestion_time", current_timestamp()) \
        .write.format("delta").mode("overwrite").save("gs://bigdata-team3-uet-zz/bronze/dim_users")

if posts_df_all:
    posts_df_all.withColumn("ingestion_time", current_timestamp()) \
        .write.format("delta").mode("overwrite").save("gs://bigdata-team3-uet-zz/bronze/dim_posts")

if hashtags_df_all:
    hashtags_df_all.withColumn("ingestion_time", current_timestamp()) \
        .write.format("delta").mode("overwrite").save("gs://bigdata-team3-uet-zz/bronze/dim_hashtags")

if post_hashtag_df_all:
    post_hashtag_df_all.withColumn("ingestion_time", current_timestamp()) \
        .write.format("delta").mode("overwrite").save("gs://bigdata-team3-uet-zz/bronze/fact_post_hashtags")

user_mentions_df_all = user_mentions_df_all.dropDuplicates() # Loại bỏ trùng lặp cuối cùng
user_mentions_df_all.withColumn("ingestion_time", current_timestamp()) \
    .write.format("delta").mode("overwrite").save("gs://bigdata-team3-uet-zz/bronze/fact_user_mentions")

print("✅ HOÀN TẤT: Đã chuẩn hóa và lưu xuống Bronze layer.")
