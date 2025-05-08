from pyspark.sql import SparkSession
from delta.tables import DeltaTable

# Đường dẫn
silver_product_path = "gs://bigdata-team3-uet-zz/silver/products"
gold_product_path = "gs://bigdata-team3-uet-zz/gold/products"

spark = SparkSession.builder \
    .appName("MergeClientsSilverToGold") \
    .master("local[*]") \
    .config("spark.jars", "/opt/spark/jars/delta-spark_2.12-3.3.1.jar") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()


# Đọc dữ liệu từ bảng silver
products_df = spark.read.format("delta").load(silver_product_path).select(
    "product_id",
    "level_1",
    "level_2",
    "level_3",
    "level_4",
    "brand_name",
    "product_name",
    "product_price",
    "first_issue_date"
)

# Loại bỏ product_id bị null (nếu có)
products_cleaned = products_df.filter("product_id IS NOT NULL")

# Kiểm tra xem bảng gold đã tồn tại hay chưa
if DeltaTable.isDeltaTable(spark, gold_product_path):
    delta_table = DeltaTable.forPath(spark, gold_product_path)

    delta_table.alias("target").merge(
        products_cleaned.alias("source"),
        "target.product_id = source.product_id"
    ).whenMatchedUpdate(set={
        "level_1": "source.level_1",
        "level_2": "source.level_2",
        "level_3": "source.level_3",
        "level_4": "source.level_4",
        "brand_name": "source.brand_name",
        "product_name": "source.product_name",
        "product_price": "source.product_price",
        "first_issue_date": "source.first_issue_date"
    }).whenNotMatchedInsert(values={
        "product_id": "source.product_id",
        "level_1": "source.level_1",
        "level_2": "source.level_2",
        "level_3": "source.level_3",
        "level_4": "source.level_4",
        "brand_name": "source.brand_name",
        "product_name": "source.product_name",
        "product_price": "source.product_price",
        "first_issue_date": "source.first_issue_date"
    }).execute()
else:
    # Nếu chưa tồn tại thì ghi lần đầu
    products_cleaned.write.format("delta").save(gold_product_path)

print("✅ Merge products từ silver lên gold đã hoàn tất.")