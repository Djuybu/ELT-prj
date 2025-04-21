import findspark
findspark.init()

import random
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

fname_list = [
    "Nguyen", "Tran", "Le", "Pham", "Hoang", "Huynh", "Phan", "Vu",
    "Vo", "Dang", "Bui", "Do", "Ho", "Ngo", "Duong", "Ly", "Trinh",
    "Mai", "Dinh", "Truong"
]

midname_list = [
    "Van", "Thi", "Ngoc", "Duc", "Huu", "Minh", "Thanh", "Quoc",
    "Tuan", "Anh", "Nhat", "Khanh", "Bao", "Gia", "Kim", "Phuong",
    "Hoang", "Nhu", "Trung", "Manh", "Thu", "Xuan", "My", "Thuy",
    "Chau", "Huyen", "Trang", "Thai", "Hai", "Tuyet", "Tan", "Lan",
    "Linh", "Son", "Quynh", "Dieu"
]

lname_list = [
    "Anh", "Hoa", "Tuan", "Trang", "Linh", "Ngoc", "Khanh", "Phuong",
    "Long", "Nam", "Hung", "Huy", "Duy", "Vy", "Hanh", "Huong",
    "Tam", "Tien", "Thao", "Loan", "My", "Son", "Lan", "Thuy",
    "Hien", "Dieu", "Trung", "Binh", "Quynh", "Nhi", "Ha", "Giang",
    "Dao", "Tin", "Toan", "Phong", "Yen", "Chi"
]




random_fname_udf = udf(lambda: random.choice(fname_list))
random_midname_udf = udf(lambda: random.choice(midname_list))
random_lname_udf = udf(lambda: random.choice(lname_list))


sparkContext = SparkContext.getOrCreate()
spark = SparkSession(sparkContext)

clients_df = spark.read.csv("./clients.csv", header=True, inferSchema=True)
clients_df = clients_df.withColumn("first_name", random_fname_udf()) \
    .withColumn("middle_name", random_midname_udf()) \
    .withColumn("last_name", random_lname_udf()) \
    
clients_df.printSchema()

# clients_df.coalesce(1).write.csv(
#     "./clients_cleaned",
#     header=True,
#     mode="overwrite",
# )

