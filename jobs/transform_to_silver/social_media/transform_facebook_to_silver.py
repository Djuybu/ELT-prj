from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, TimestampType
import os
import random
from datetime import datetime, timedelta

# -------------------------------------
# ⚙️ 1. Khởi tạo Spark Session
# -------------------------------------
spark = SparkSession.builder \
    .appName("NormalizeFacebookData") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# -------------------------------------
# 📁 2. Đường dẫn dữ liệu
# -------------------------------------
bronze_path = "gs://bigdata-team3-uet-zz/bronze/social_media/facebook"
silver_path = "gs://bigdata-team3-uet-zz/silver/social_media/facebook"

# -------------------------------------
# 📥 3. Đọc dữ liệu từ bronze
# -------------------------------------
facebook_df = spark.read.format("delta").load(bronze_path)

# -------------------------------------
# 🧱 4. Định nghĩa schema silver
# -------------------------------------
user_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("display_name", StringType(), True),
    StructField("biography", StringType(), True),
    StructField("is_verified", BooleanType(), True),
    StructField("following", IntegerType(), True),
    StructField("followed", IntegerType(), True),
    StructField("post", IntegerType(), True),
    StructField("user_type", StringType(), True),
])

post_schema = StructType([
    StructField("post_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("post_content", StringType(), True),
    StructField("post_date", TimestampType(), True),
    StructField("post_url", StringType(), True),
    StructField("post_type", StringType(), True),
])

# -------------------------------------
# 💬 5. Các câu comment mẫu
# -------------------------------------
positive_comments = [
    "This is good!", "Absolutely love it!", "Highly recommend!", "Exceeded my expectations!",
    "Fantastic quality!", "Thật tuyệt vời!", "Sản phẩm chất lượng cao!", "Sẽ mua lại!",
    "Excelente producto!", "Très bon produit!", "Лучший продукт!", "Sehr gut!",
    "صناعة ممتازة!", "굉장해요!"
]

negative_comments = [
    "This is bad!", "I absolutely hate it!", "Would not recommend!", "Terrible quality!",
    "Thật tệ!", "Sản phẩm kém chất lượng!", "Sẽ không mua lại!",
    "Producto pésimo!", "Très mauvais produit!", "Худший продукт!", "Sehr schlecht!",
    "ممتاز جدا!", "완전 별로에요!"
]

def get_random_comment():
    return random.choice(positive_comments + negative_comments)

# -------------------------------------
# 👤 6. Tạo user từ mỗi dòng Facebook
# -------------------------------------
def create_user_from_row(row):
    return (
        row.user_id,
        row.user_name,
        row.user_url,
        True,
        random.randint(100, 1000),
        random.randint(100, 1000),
        random.randint(10, 500),
        "Facebook"
    )

# -------------------------------------
# 📝 7. Tạo post từ mỗi dòng Facebook
# -------------------------------------
def create_post_from_row(row):
    return (
        row.post_id,
        row.user_id,
        get_random_comment(),
        datetime.now() - timedelta(days=1),
        row.post_url,
        "Facebook"
    )

# -------------------------------------
# 🔁 8. Chuyển đổi thành DataFrame silver
# -------------------------------------
user_rdd = facebook_df.rdd.map(create_user_from_row)
user_df = spark.createDataFrame(user_rdd, user_schema)

post_rdd = facebook_df.rdd.map(create_post_from_row)
post_df = spark.createDataFrame(post_rdd, post_schema)

# -------------------------------------
# 📣 9. Sinh user mentions
# -------------------------------------
def generate_user_mentions(num_mentions, user_df, post_df):
    user_ids = user_df.select("user_id").rdd.flatMap(lambda x: x).collect()
    post_ids = post_df.select("post_id").rdd.flatMap(lambda x: x).collect()
    
    mentions = [
        (random.choice(post_ids), random.choice(user_ids))
        for _ in range(num_mentions)
    ]
    return spark.createDataFrame(mentions, ["post_id", "mentioned_user_id"])

mentions_df = generate_user_mentions(100, user_df, post_df)

# -------------------------------------
# 💾 10. Ghi dữ liệu ra tầng silver
# -------------------------------------
user_df.write.format("delta").mode("overwrite").save(os.path.join(silver_path, "user"))
post_df.write.format("delta").mode("overwrite").save(os.path.join(silver_path, "post"))
mentions_df.write.format("delta").mode("overwrite").save(os.path.join(silver_path, "user_mentions"))

print("✅ Dữ liệu Facebook đã được chuẩn hóa và lưu vào silver thành công.")
