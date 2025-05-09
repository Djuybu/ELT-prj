from pyspark.sql import SparkSession
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
bronze_path = "gs://bigdata-team3-uet-zz/bronze/social_media/instagram"
# Đường dẫn đến thư mục chứa dữ liệu đã chuẩn hóa
silver_path = "gs://bigdata-team3-uet-zz/silver/social_media/"

facebook_df = spark.read.format("delta").load(bronze_path, header=True, inferSchema=True)

from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, TimestampType, ArrayType

userSchema = StructType([
    StructField("user_id", StringType(), True),
    StructField("display_name", StringType(), True),
    StructField("biography", StringType(), True),
    StructField("is_verified", BooleanType(), True),
    StructField("following", IntegerType(), True),
    StructField("followed", IntegerType(), True),
    StructField("post", IntegerType(), True),
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
insta_df = spark.read.format("delta").load(bronze_path)
def create_user_from_instagram_row(row):
    """
    Create new user from instagram row:
    - user_id: random 12-digit number
    - display_name: extract from comment_user_url: "https://www.instagram.com/username/"
    - biography: comment_user_url
    - is_verified: True
    - following: random number from 0 to 1000
    - followed: random number from 0 to 1000
    - post: random number from 0 to 1000
    """
    user_id = random.randint(100000000000, 999999999999)  # Random 12-digit number
    display_name = row.comment_user_url.split("/")[-2]
    biography = row.comment_user_url
    is_verified = True
    # Sinh ngẫu nhiên trong phạm vi từ 0 đến 1000
    following = random.randint(0, 1000)
    followed = random.randint(0, 1000)
    post = random.randint(0, 1000)
    return (user_id, display_name, biography, is_verified, following, followed, post)

from datetime import datetime, timedelta
from pyspark.sql.types import StructType, StructField, StringType, TimestampType


user_insta_rdd = insta_df.rdd.map(lambda row: create_user_from_instagram_row(row))
# Chuyển sang DataFrame mới
user_insta_df = spark.createDataFrame(user_insta_rdd, userSchema)


# 1. Chuẩn bị dictionary map bio -> user_id
biography_to_user_id = dict(
    user_insta_df.select('biography', 'user_id')
                .rdd
                .map(lambda row: (row['biography'], row['user_id']))
                .collect()
)

current_time = datetime.now()

def create_post_from_instagram_row(row, current_time, bio_userid_dict):
    post_id = row.post_id
    user_id = bio_userid_dict.get(row.comment_user_url, None)
    post_content = create_post_content()
    post_date = current_time - timedelta(days=1)
    post_url = row.post_url
    post_type = "Instagram"
    return (post_id, user_id, post_content, post_date, post_url, post_type)


post_insta_rdd = insta_df.rdd.map(
    lambda row: create_post_from_instagram_row(row, current_time, biography_to_user_id)
)
# Chuyển sang DataFrame mới
post_insta_df = spark.createDataFrame(post_insta_rdd, PostSchema)


import random

def create_hashtag_from_instagram_row(row):
    """
    Tạo hastag từ dòng Instagram, mỗi hashtag duy nhất, loại bỏ hashtag mẫu 'null_hehe"_hehe'.
    - hastag_id: số ngẫu nhiên 12 chữ số
    - hashtag_text: từ hashtag_comment, tách bởi "," sau khi đã loại bỏ dấu "[" và "]"
    """
    # Kiểm tra null hoặc empty
    if row.hashtag_comment is None:
        return []

    hashtag_comment = row.hashtag_comment.strip()
    if hashtag_comment.lower() == 'null' or hashtag_comment == '' or hashtag_comment == 'unknown' or hashtag_comment == ' ':
        return []

    # Loại bỏ dấu "[" và "]"
    hashtag_comment = hashtag_comment[1:-1]

    # Tách thành list các hashtag
    hashtag_text = [h.strip().strip('"') for h in hashtag_comment.split(",")]

    # Loại bỏ các hashtag có giá trị 'null_hehe"_hehe'
    hashtag_text = [h for h in hashtag_text if h != 'null_hehe"_hehe']

    # Loại bỏ trùng lặp bằng set
    unique_hashtags = set(hashtag_text)

    hastag_id = random.randint(100000000000, 999999999999)

    # Trả về danh sách các cặp (hastag_id, hashtag), mỗi hashtag duy nhất
    return [(hastag_id, h) for h in unique_hashtags]

hashtag_insta_rdd = insta_df.rdd.flatMap(lambda row: create_hashtag_from_instagram_row(row))
# Chuyển sang DataFrame mới
hashtag_insta_df = spark.createDataFrame(hashtag_insta_rdd, ["hastag_id", "hashtag_text"])

def parse_hashtags(hashtag_str):
    if hashtag_str is None:
        return []

    text = hashtag_str.strip()
    # Bỏ dấu ngoặc `[ ]` nếu có
    text = text.strip('[]')

    # Chọn separator phù hợp — nếu nhiều dòng có thể
    # Ví dụ kiểm tra xem có ";" hay "," để tách
    separator = ','
    if ';' in text:
        separator = ';'

    parts = text.split(separator)
    result = []

    for part in parts:
        part = part.strip().strip('"')
        if ':' in part:
            key, value = part.split(':', 1)
            key = key.strip()
            value = value.strip()
            # bỏ qua null, rỗng
            if key and value and value.lower() != 'null' and value != 'null_hehe"_hehe':
                result.append((key, value))
    return result

# Áp dụng cho từng row trong DataFrame hoặc duyệt thủ công
hashtags_list = []
for row in insta_df.select('post_id', 'hashtag_comment') \
        .filter((col('hashtag_comment').isNotNull()) & (col('hashtag_comment') != 'unknown')) \
        .distinct().collect():
    post_id = row['post_id']
    hashtags_str = row['hashtag_comment']
    pairs = parse_hashtags(hashtags_str)
    # Giờ có list các cặp `(a,b)` rồi
    # Có thể muốn lưu vào dict, hoặc chuyển về DataFrame
    hashtags_list.extend([{'post_id': post_id, 'key': k, 'value': v} for k, v in pairs])

parse_hashtags_udf = spark.udf.register("parse_hashtags", parse_hashtags, ArrayType(StructType([
    StructField("key", StringType(), True),
    StructField("value", StringType(), True)
])))
#Conver thành dict
hashtag_post_dict = dict(
    hashtag_insta_df.select('hastag_id', 'hashtag_text')
        .rdd
        .map(lambda row: (row['hastag_id'], row['hashtag_text']))
        .filter(lambda x: x[1] is not None and x[1].strip().lower() != 'null' and x[1] != 'unknown')  # Lọc bỏ giá trị không hợp lệ
        .collect()
)


# Tương tự cho hashtag_id_dict
hashtag_id_dict = dict(
    hashtag_insta_df.select('hastag_id', 'hashtag_text')
        .rdd
        .map(lambda row: (row['hastag_id'], row['hashtag_text']))
        .filter(lambda x: x[1] is not None and x[1].strip().lower() != 'null' and x[1] != 'unknown')  # Lọc bỏ giá trị không hợp lệ
        .collect()
)

import random

def generate_user_mentions(instance_nums):
    """
    Generate random user mentions for a post.
    Get all user_ids from user_insta_df and post_id from post_insta_df
    For instance_nums, randomly select post_id and mentioned_user_id
    """
    # Lấy user_ids và post_ids 1 lần duy nhất
    user_ids = user_insta_df.select('user_id').rdd.flatMap(lambda x: x).collect()
    post_ids = post_insta_df.select('post_id').rdd.flatMap(lambda x: x).collect()

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

user_insta_df.write.format("delta").mode("append").save(silver_path + "user")
post_insta_df.write.format("delta").mode("append").save(silver_path + "post")
hashtag_insta_df.write.format("delta").mode("append").save(silver_path + "hashtag")
user_mentions_df.write.format("delta").mode("append").save(silver_path + "user_mentions")

