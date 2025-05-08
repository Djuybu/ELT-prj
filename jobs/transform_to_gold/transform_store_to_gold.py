from pyspark.sql import SparkSession
from delta.tables import DeltaTable


spark = SparkSession.builder \
    .appName("MergeClientsSilverToGold") \
    .master("local[*]") \
    .config("spark.jars", "/opt/spark/jars/delta-spark_2.12-3.3.1.jar") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()


# Đường dẫn
silver_store_path = "gs://bigdata-team3-uet-zz/silver/stores"
gold_store_path = "gs://bigdata-team3-uet-zz/gold/stores"

# Đọc dữ liệu từ bảng silver
stores_df = spark.read.format("delta").load(silver_store_path).select(
    "store_id",
    "store_name",
    "store_location",
    "region",
    "store_format",
    "first_issue_date"
)

# Loại bỏ store_id bị null
stores_cleaned = stores_df.filter("store_id IS NOT NULL")

# Kiểm tra xem bảng gold đã tồn tại hay chưa
if DeltaTable.isDeltaTable(spark, gold_store_path):
    delta_table = DeltaTable.forPath(spark, gold_store_path)

    delta_table.alias("target").merge(
        stores_cleaned.alias("source"),
        "target.store_id = source.store_id"
    ).whenMatchedUpdate(set={
        "store_name": "source.store_name",
        "store_location": "source.store_location",
        "region": "source.region",
        "store_format": "source.store_format",
        "first_issue_date": "source.first_issue_date"
    }).whenNotMatchedInsert(values={
        "store_id": "source.store_id",
        "store_name": "source.store_name",
        "store_location": "source.store_location",
        "region": "source.region",
        "store_format": "source.store_format",
        "first_issue_date": "source.first_issue_date"
    }).execute()
else:
    # Nếu chưa tồn tại thì ghi lần đầu
    stores_cleaned.write.format("delta").save(gold_store_path)

print("✅ Merge stores từ silver lên gold đã hoàn tất.")