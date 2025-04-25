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
silver_path = "gs://bigdata-team3-uet-zz/silver/clients"
gold_path = "gs://bigdata-team3-uet-zz/gold/clients"

# Đọc dữ liệu từ bảng silver
clients_df = spark.read.format("delta").load(silver_path).select(
    "client_id",
    "first_issue_date",
    "first_redeem_date",
    "age",
    "gender"
)

# Loại bỏ client_id bị null (nếu có)
clients_cleaned = clients_df.filter("client_id IS NOT NULL")

# Merge vào bảng gold nếu đã tồn tại
if DeltaTable.isDeltaTable(spark, gold_path):
    delta_table = DeltaTable.forPath(spark, gold_path)

    delta_table.alias("target").merge(
        clients_cleaned.alias("source"),
        "target.client_id = source.client_id"
    ).whenMatchedUpdate(set={
        "first_issue_date": "source.first_issue_date",
        "first_redeem_date": "source.first_redeem_date",
        "age": "source.age",
        "gender": "source.gender"
    }).whenNotMatchedInsert(values={
        "client_id": "source.client_id",
        "first_issue_date": "source.first_issue_date",
        "first_redeem_date": "source.first_redeem_date",
        "age": "source.age",
        "gender": "source.gender"
    }).execute()
else:
    # Nếu chưa tồn tại thì ghi lần đầu
    clients_cleaned.write.format("delta").save(gold_path)

print("✅ Merge clients từ silver lên gold đã hoàn tất.")
