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
bronze_path = "bigdata-team3-uet-zz/bronze/social_media/facebook"
# Đường dẫn đến thư mục chứa dữ liệu đã chuẩn hóa
silver_path = "bigdata-team3-uet-zz/silver/social_media/"

facebook_df = spark.read.format("delta").load(bronze_path)

from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, TimestampType

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

from datetime import datetime, timedelta
def create_post_from_facebook_row(row):
    """
    create post from face_df row, with primary key being post_id row (make sure they are unique):
    - post_id: from post_id
    - user_id: from user_id
    - post_content: use create_post_content
    - post_date: current_datetime - 1
    - post_url: post_url
    """
    post_id = row.post_id
    user_id = row.user_id
    post_content = create_post_content()
    post_date = datetime.now() - timedelta(days=1)
    post_url = row.post_url
    post_type = "Facebook"

    return (post_id, user_id, post_content, post_date, post_url, post_type)


import random

def create_user_from_facebook_row(row):
    user_id = row.user_id
    display_name = row.user_name
    biography = row.user_url
    is_verified = True
    # Sinh ngẫu nhiên trong phạm vi từ 0 đến 1000
    following = random.randint(0, 1000)
    followed = random.randint(0, 1000)
    post = random.randint(0, 1000)

    return (user_id, display_name, biography, is_verified, following, followed, post)

user_face_rdd = facebook_df.rdd.map(lambda row: create_user_from_facebook_row(row))

# Chuyển sang DataFrame mới
user_face_df = spark.createDataFrame(user_face_rdd, userSchema)


post_face_rdd = facebook_df.rdd.map(lambda row: create_post_from_facebook_row(row))

# Chuyển sang DataFrame mới
post_face_df = spark.createDataFrame(post_face_rdd, PostSchema)

user_face_df.write.format("delta").mode("overwrite").save(os.path.join(silver_path, "user"))
post_face_df.write.format("delta").mode("overwrite").save(os.path.join(silver_path, "post"))