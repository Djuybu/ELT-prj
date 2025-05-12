from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, lit, split, trim, lower, current_timestamp, monotonically_increasing_id, min as spark_min, array, explode
from pyspark.sql.types import StringType, LongType, StructType, StructField
from pyspark.sql.functions import udf
import os
import random
from datetime import datetime, timedelta


# ⚙️ Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("NormalizeSocialMedia") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Đường dẫn đến thư mục chứa dữ liệu thô
bronze_path = "gs://bigdata-team3-uet-zz/bronze/social_media/twitter"
# Đường dẫn đến thư mục chứa dữ liệu đã chuẩn hóa
silver_path = "gs://bigdata-team3-uet-zz/silver/social_media/"

twitter_df = spark.read.format("delta").load(bronze_path)

from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, TimestampType

userSchema = StructType([
    StructField("user_id", StringType(), True),
    StructField("display_name", StringType(), True),
    StructField("biography", StringType(), True),
    StructField("is_verified", BooleanType(), True),
    StructField("following", IntegerType(), True),
    StructField("followed", IntegerType(), True),
    StructField("post", IntegerType(), True),
    StructField("user_type", StringType(), True),
])

PostSchema = StructType([
    StructField("post_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("post_content", StringType(), True),
    StructField("post_date", TimestampType(), True),
    StructField("post_url", StringType(), True),
    StructField("post_type", StringType(), True),
])

positive_comment_for_product = [
    "This is good!",
    "Absolutely love it!",
    "Highly recommend!",
    "Exceeded my expectations!",
    "Fantastic quality!",
    "Thật tuyệt vời!",
    "Sản phẩm chất lượng cao!",
    "Hết lời khen, xuất sắc!",
    "Dịch vụ tuyệt vời!",
    "Sẽ mua lại!",
    "Excelente producto!",
    "¡Muy recomendable!",
    "Me encanta este producto!",
    "Qualidad excepcional!",
    "Una compra inteligente!",
    "Très bon produit!",
    "Je l'adore!",
    "Recommande vivement!",
    "Excellente qualité!",
    "Un achat qui vaut son prix!",
    "Prodotto eccezionale!",
    "Assolutamente raccomandato!",
    "Superbe qualità!",
    "C'est fantastique!",
    "Лучший продукт!",
    "Абсолютно рекомендую!",
    "Отличное качество!",
    "Замечательный выбор!",
    "Sehr gut!",
    "Hervorragende Qualität!",
    "Stark empfohlen!",
    "صناعة ممتازة!",
    "أنصح به بشدة!",
    "ممتاز جدا!",
    "굉장해요!",
    "최고의 선택입니다!"
]

negative_comment_for_product = [
    "This is bad!",
    "I absolutely hate it!",
    "Would not recommend!",
    "Expectations not met!",
    "Terrible quality!",
    "Thật tệ!",
    "Sản phẩm kém chất lượng!",
    "Không có gì để khen, tệ hại!",
    "Dịch vụ rất kém!",
    "Sẽ không mua lại!",
    "Producto pésimo!",
    "¡Muy decepcionante!",
    "No me gusta este producto!",
    "Calidad muy pobre!",
    "Una compra desafortunada!",
    "Très mauvais produit!",
    "Je déteste!",
    "Not recommended at all!",
    "Qualité médiocre!",
    "Un achat regrettable!",
    "Prodotto pessimo!",
    "Assolutamente da evitare!",
    "Péssima qualità!",
    "C'est décevant!",
    "Худший продукт!",
    "Никогда не покупайте!",
    "Качество отвратительное!",
    "Выбор ужасный!",
    "Sehr schlecht!",
    "Völlig unbrauchbar!",
    "Nicht zu empfehlen!"
]

def create_post_content():
    """
    Choose a random from positive/negative comments
    """
    comments = positive_comment_for_product + negative_comment_for_product
    return random.choice(comments)

from datetime import datetime, timedelta
def create_user_from_twitter_row(user_posted):
    """
    Create a new user from unique user_posted value:
    - user_id: random 12-digit number
    - display_name: user_posted
    - biography: http://x.com/user_posted
    - is_verified: True
    - following: random number from 0 to 1000
    - followed: random number from 0 to 1000
    - post: random number from 0 to 1000
    """
    user_id = random.randint(100000000000, 999999999999)
    display_name = user_posted
    biography = f"http://x.com/{user_posted}"
    is_verified = True
    following = random.randint(0, 1000)
    followed = random.randint(0, 1000)
    post = random.randint(0, 1000)
    type = "Twitter"
    return Row(user_id=user_id, display_name=display_name, biography=biography,
               is_verified=is_verified, following=following, followed=followed, post=post, user_type=type)


import random

unique_users_rdd = twitter_df.select("user_posted").distinct().rdd.map(lambda row: create_user_from_twitter_row(row.user_posted))

user_df = spark.createDataFrame(unique_users_rdd, userSchema)

user_id_dict = dict(
    user_df.select('user_id', 'display_name')
        .rdd
        .map(lambda row: (row['user_id'], row['display_name']))
        .collect()
)
print(user_id_dict)

import re
import random
from datetime import datetime, timedelta

def create_post_from_twitter_row(row):
    """
    Create a new post from a Twitter row:
    - post_id, user_id, post_content, post_date, post_url, post_type
    """
    post_id = row.id
    post_url = row.url
    
    # Trích xuất user_id từ URL dạng https://x.com/{user_id}/status/{post_id}
    extracted_user_id = None
    if post_url and isinstance(post_url, str):
        match = re.search(r'https://x\.com/([^/]+)/status/', post_url)
        if match:
            extracted_user_id = match.group(1)

    # Tìm user_id từ dict dựa theo extracted_user_id
    if extracted_user_id:
        user_id = next(
            (uid for uid, name in user_id_dict.items() if name and name.lower() == extracted_user_id.lower()), 
            None
        )
    else:
        user_id = None

    # Nếu không tìm thấy user_id, chọn ngẫu nhiên
    if user_id is None:
        user_id = random.choice(list(user_id_dict.keys()))

    # Tạo nội dung bài đăng
    post_content = create_post_content()

    # Thời gian bài đăng: hiện tại - 1 ngày
    post_date = datetime.now() - timedelta(days=1)

    # Loại bài đăng
    post_type = "Twitter"

    return (post_id, user_id, post_content, post_date, post_url, post_type)

# tạo từ các row của twitter_df
post_tw_rdd = twitter_df.rdd.map(lambda row: create_post_from_twitter_row(row))

# Chuyển sang DataFrame mới
post_tw_df = spark.createDataFrame(post_tw_rdd, PostSchema)

def create_hashtag_from_twitter_row(row):
    """
    Create a new hashtag from Twitter row:
    - hastag_id: random 12-digit number
    - hashtag_text: from hashtags, split by ","
    """
    # Kiểm tra null hoặc empty
    if row.hashtags is None:
        return []

    hashtags = row.hashtags.strip()
    if hashtags.lower() == 'null' or hashtags == '' or hashtags == 'unknown' or hashtags == ' ':
        return []

    # Tách thành list các hashtag
    hashtag_text = [h.strip().strip('"') for h in hashtags.split(",")]

    # Loại bỏ các hashtag có giá trị 'null_hehe"_hehe'
    hashtag_text = [h for h in hashtag_text if h != 'null_hehe"_hehe']

    # Loại bỏ trùng lặp bằng set
    unique_hashtags = set(hashtag_text)

    hastag_id = random.randint(100000000000, 999999999999)

    # Trả về danh sách các cặp (hastag_id, hashtag), mỗi hashtag duy nhất
    return [(hastag_id, h) for h in unique_hashtags]    

hashtag_tw_rdd = twitter_df.rdd.flatMap(lambda row: create_hashtag_from_twitter_row(row))
# Chuyển sang DataFrame mới
hashtag_tw_df = spark.createDataFrame(hashtag_tw_rdd, ["hastag_id", "hashtag_text"])
# Hiển thị kết quả
hashtag_tw_df.show(truncate=False)

hashtag_id_dict = dict(
    hashtag_tw_df.select('hastag_id', 'hashtag_text')
        .rdd
        .map(lambda row: (row['hastag_id'], row['hashtag_text']))
        .filter(lambda x: x[1] is not None and x[1].strip().lower() != 'null' and x[1] != 'unknown')  # Lọc bỏ giá trị không hợp lệ
        .collect()
)

post_hashtag_dict = dict(
    hashtag_tw_df.select('hastag_id', 'hashtag_text')
        .rdd
        .map(lambda row: (row['hastag_id'], row['hashtag_text']))
        .filter(lambda x: x[1] is not None and x[1].strip().lower() != 'null' and x[1] != 'unknown')  # Lọc bỏ giá trị không hợp lệ
        .collect()
)

def create_post_hashtag():
    """
    From hashtag_id_dict and post_hashtag_dict, create a new post_hashtag row:
    - post_id: from post_id
    - hastag_id: from hastag_id
    """
    result = []

    # Đảo ngược hashtag_id_dict: group các hashtag_id theo chuỗi nội dung
    value_to_hashtag_ids = {}
    for hashtag_id, value in hashtag_id_dict.items():
        if value:  # Bỏ qua chuỗi rỗng
            value_to_hashtag_ids.setdefault(value, []).append(hashtag_id)

    # Duyệt các post_id, nếu chuỗi không rỗng và tồn tại trong hashtag_id_dict, tạo cặp
    for post_id, value in post_hashtag_dict.items():
        if value and value in value_to_hashtag_ids:
            for hashtag_id in value_to_hashtag_ids[value]:
                result.append((post_id, hashtag_id))

    return result

# Tạo DataFrame từ danh sách các cặp (post_id, hastag_id)
post_hashtag_list = create_post_hashtag()
post_hashtag_df = spark.createDataFrame(post_hashtag_list, ["post_id", "hastag_id"])

def generate_user_mentions(instance_nums):
    """
    Generate random user mentions for a post.
    Get all user_ids from user_insta_df and post_id from post_insta_df
    For instance_nums, randomly select post_id and mentioned_user_id
    """
    # Lấy user_ids và post_ids 1 lần duy nhất
    user_ids = user_df.select('user_id').rdd.flatMap(lambda x: x).collect()
    post_ids = post_tw_df.select('post_id').rdd.flatMap(lambda x: x).collect()

    mentions_list = []
    for _ in range(instance_nums):    
        post_id = random.choice(post_ids)
        mentioned_user_id = random.choice(user_ids)
        mentions_list.append((post_id, mentioned_user_id))

    return mentions_list

#generate user mentions
user_mentions = generate_user_mentions(100)
# Convert to DataFrame
user_mentions_df = spark.createDataFrame(user_mentions, ["post_id", "mentioned_user_id"])

# Lưu DataFrame vào Delta Lake
user_df.write.format("delta").mode("append").save(os.path.join(silver_path, "user"))
post_tw_df.write.format("delta").mode("append").save(os.path.join(silver_path, "post"))
hashtag_tw_df.write.format("delta").mode("append").save(os.path.join(silver_path, "hashtag"))
post_hashtag_df.write.format("delta").mode("append").save(os.path.join(silver_path, "post_hashtag"))
user_mentions_df.write.format("delta").mode("append").save(os.path.join(silver_path, "user_mentions"))