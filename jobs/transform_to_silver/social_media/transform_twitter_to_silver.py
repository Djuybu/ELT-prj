from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col
from pyspark.sql.types import *
import random
import re
from datetime import datetime, timedelta
import os

# === Spark session setup ===
spark = SparkSession.builder \
    .appName("NormalizeTwitterData") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# === Paths ===
bronze_path = "gs://bigdata-team3-uet-zz/bronze/social_media/twitter"
silver_path = "gs://bigdata-team3-uet-zz/silver/social_media/"

# === Schemas ===
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

# === Read data ===
twitter_df = spark.read.format("delta").load(bronze_path)

# === Sample comments ===
positive_comments = ["Highly recommend!", "Fantastic quality!", "Sẽ mua lại!", "Très bon produit!", "Лучший продукт!"]
negative_comments = ["Terrible quality!", "Không mua lại!", "Très mauvais produit!", "Худший продукт!"]

# === Helper functions ===

name_list = [
    "Darth Vader", "Luke Skywalker", "Leia Organa", "Han Solo", "Yoda", "Obi-Wan Kenobi", "Chewbacca",
    "R2-D2", "C-3PO", "Padmé Amidala", "Anakin Skywalker", "Boba Fett", "Darth Maul", "Kylo Ren", "Rey",
    "Finn", "Poe Dameron", "BB-8", "Jyn Erso", "Cassian Andor", "Saw Gerrera", "Mon Mothma", "Ahsoka Tano",
    "Ezra Bridger", "Kanan Jarrus", "Sabine Wren"
]

def create_user_from_twitter_row(user_posted):
    """
    Create a new user from unique user_posted value.
    If user_posted is None, assign a random name from name_list.
    """
    user_id = random.randint(100000000000, 999999999999)

    # If user_posted is None, pick a name from the list
    if user_posted is None or str(user_posted).strip() == "":
        display_name = random.choice(name_list)
    else:
        display_name = user_posted

    biography = f"http://x.com/{display_name.replace(' ', '').lower()}"
    is_verified = True
    following = random.randint(0, 1000)
    followed = random.randint(0, 1000)
    post = random.randint(0, 1000)

    return Row(
        user_id=user_id,
        display_name=display_name,
        biography=biography,
        is_verified=is_verified,
        following=following,
        followed=followed,
        post=post
    )


# === Generate users ===
unique_users_rdd = twitter_df.select("user_posted").distinct().rdd.map(lambda row: create_user_from_twitter_row(row.user_posted))

user_df = spark.createDataFrame(unique_users_rdd, userSchema)
user_id_map = dict(user_df.select("user_id", "display_name").rdd.map(lambda r: (r.display_name.lower(), r.user_id)).collect())

user_id_dict = dict(
    user_df.select('user_id', 'display_name')
        .rdd
        .map(lambda row: (row['user_id'], row['display_name']))
        .collect()
)

def create_post_content():
    """
    Choose a random from positive/negative comments
    """
    comments = positive_comment_for_product + negative_comment_for_product
    return random.choice(comments)

# === Generate posts ===
def create_post_from_twitter_row(row):
    """
    Create a new post from Twitter row:
    - post_id: distinct id from the id row of tw_df
    - user_id: from the url: https://x.com/billboard/status/1868159094567121215. Extract the name and find the username from user_id_dict. If url not find, get a random id from user_df
    - post_content: create_post_content()
    - post_date: current_datetime - 1
    - post_url: url
    - post_type: Twitter
    """
    post_id = row.id
    user_id = user_id_dict.get(row.user_posted, random.choice(list(user_id_dict.keys())))
    post_content = create_post_content()
    post_date = datetime.now() - timedelta(days=1)
    post_url = row.url
    post_type = "Twitter"
    return (post_id, user_id, post_content, post_date, post_url, post_type)

post_rdd = twitter_df.rdd.map(create_post_from_twitter_row)
post_df = spark.createDataFrame(post_rdd, PostSchema)

# === Generate hashtags ===
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
hashtag_df = spark.createDataFrame(hashtag_tw_rdd, ["hastag_id", "hashtag_text"])
# Hiển thị kết quả

hashtag_id_dict = dict(
    hashtag_df.select('hastag_id', 'hashtag_text')
        .rdd
        .map(lambda row: (row['hastag_id'], row['hashtag_text']))
        .filter(lambda x: x[1] is not None and x[1].strip().lower() != 'null' and x[1] != 'unknown')  # Lọc bỏ giá trị không hợp lệ
        .collect()
)

post_hashtag_dict = dict(
    hashtag_df.select('hastag_id', 'hashtag_text')
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

list_post_hashtag = create_post_hashtag()
print(list_post_hashtag)
# 2. Tạo RDD từ danh sách
post_hashtag_rdd = spark.sparkContext.parallelize(list_post_hashtag)
# 3. Chuyển sang DataFrame
post_hashtag_df = spark.createDataFrame(post_hashtag_rdd, ["post_id", "hashtag_id"])

# === Generate user mentions ===
def generate_mentions(count):
    post_ids = post_df.select("post_id").rdd.flatMap(lambda x: x).collect()
    user_ids = user_df.select("user_id").rdd.flatMap(lambda x: x).collect()
    return [(random.choice(post_ids), random.choice(user_ids)) for _ in range(count)]

mentions_df = spark.createDataFrame(generate_mentions(100), ["post_id", "mentioned_user_id"])

# === Save to Delta Lake ===
user_df.write.format("delta").mode("append").save(os.path.join(silver_path, "user"))
post_df.write.format("delta").mode("append").save(os.path.join(silver_path, "post"))
hashtag_df.write.format("delta").mode("append").save(os.path.join(silver_path, "hashtag"))
post_hashtag_df.write.format("delta").mode("append").save(os.path.join(silver_path, "post_hashtag"))
mentions_df.write.format("delta").mode("append").save(os.path.join(silver_path, "user_mentions"))
