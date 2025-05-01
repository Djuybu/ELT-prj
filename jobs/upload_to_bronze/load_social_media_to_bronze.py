from pyspark import SparkContext
from pyspark.sql import SparkSession
import os

# Khởi tạo SparkSession với hỗ trợ Delta Lake và GCS
sc = SparkContext.getOrCreate()
spark = SparkSession.builder \
    .appName("BronzeSocialMediaIngestion") \
    .master("local[*]") \
    .config("spark.jars", "/opt/spark/jars/delta-spark_2.12-3.3.1.jar,/opt/spark/jars/gcs-connector-hadoop3-2.2.4-shaded.jar") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "false") \
    .getOrCreate()

# Hàm nạp từng file CSV và ghi ra GCS ở định dạng Delta
def load_and_write(file_path: str, platform: str):
    print(f"🚀 Loading {platform} from {file_path}")
    df = spark.read.option("header", True).csv(file_path)
    df.write.format("delta").mode("overwrite") \
        .save(f"gs://bigdata-team3-uet-zz/bronze/social-media/{platform}")
    print(f"✅ Done writing {platform} to Bronze layer.")

if __name__ == "__main__":
    base_path = "/app/airflow/docker/ELT-prj/files"
    file_map = {
        "facebook": os.path.join(base_path, "Facebook-datasets.csv"),
        "instagram": os.path.join(base_path, "Instagram-datasets.csv"),
        "tiktok": os.path.join(base_path, "TikTok-datasets.csv"),
        "twitter": os.path.join(base_path, "Twitter- datasets.csv"),  # lưu ý: giữ đúng dấu cách nếu tên file có!
    }

    for platform, file_path in file_map.items():
        if os.path.exists(file_path):
            load_and_write(file_path, platform)
        else:
            print(f"⚠️ File not found: {file_path}")

    print("🎉 All social media datasets written to Bronze layer.")
