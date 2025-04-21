import findspark
findspark.init()
import pandas as pd
import random
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import udf, substr, col, when, trim, lit
from pyspark.sql.types import StringType

location_list = {
    "Hanoi": ["Hoan Kiem", "Ba Dinh", "Dong Da", "Hai Ba Trung", "Hoang Mai"],
    "Ho Chi Minh": ["District 1", "District 2", "District 3", "District 4", "District 5"],
    "Da Nang": ["Hai Chau", "Thanh Khe", "Lien Chieu", "Ngu Hanh Son", "Son Tra"],
    "Hai Phong": ["Hong Bang", "Ngo Quyen", "Le Chan", "Kien An", "Duong Kinh"],
    "Can Tho": ["Ninh Kieu", "Binh Thuy", "O Mon", "Thot Not", "Phong Dien"],
    "Nha Trang": ["Loc Tho", "Vinh Hoa", "Vinh Nguyen", "Vinh Hai", "Phuoc Hai"],
    "Vung Tau": ["Ward 1", "Ward 2", "Ward 3", "Ward 4", "Thang Tam"],
    "Quang Ninh": ["Ha Long", "Cam Pha", "Uong Bi", "Mong Cai", "Dong Trieu"],
    "Thanh Hoa": ["Thanh Hoa City", "Sam Son", "Bim Son", "Nong Cong", "Tho Xuan"],
    "Thai Nguyen": ["Thai Nguyen City", "Song Cong", "Pho Yen", "Phu Luong", "Dai Tu"],
    "Nghe An": ["Vinh", "Cua Lo", "Hoang Mai", "Quy Chau", "Quy Hop"],
    "Ha Tinh": ["Ha Tinh City", "Hong Linh", "Ky Anh", "Cam Xuyen", "Thach Ha"],
    "Binh Duong": ["Thu Dau Mot", "Di An", "Thuan An", "Ben Cat", "Tan Uyen"],
    "Kien Giang": ["Rach Gia", "Ha Tien", "Phu Quoc", "Hon Dat", "An Bien"],
    "Bac Giang": ["Bac Giang City", "Lang Giang", "Yen Dung", "Hiep Hoa", "Tan Yen"],
    "Bac Ninh": ["Bac Ninh City", "Tu Son", "Que Vo", "Thuan Thanh", "Tien Du"],
    "Nam Dinh": ["Nam Dinh City", "My Loc", "Y Yen", "Hai Hau", "Giao Thuy"],
    "Ninh Binh": ["Ninh Binh City", "Tam Diep", "Gia Vien", "Hoa Lu", "Kim Son"],
    "Ha Nam": ["Phu Ly", "Duy Tien", "Binh Luc", "Kim Bang", "Thanh Liem"],
    "Vinh Phuc": ["Vinh Yen", "Phuc Yen", "Lap Thach", "Tam Duong", "Yen Lac"],
    "Quang Binh": ["Dong Hoi", "Ba Don", "Bo Trach", "Quang Ninh", "Le Thuy"],
    "Quang Tri": ["Dong Ha", "Quang Tri Town", "Vinh Linh", "Hai Lang", "Gio Linh"],
    "Thua Thien Hue": ["Hue", "Huong Thuy", "Huong Tra", "Phong Dien", "Phu Vang"],
    "Binh Thuan": ["Phan Thiet", "La Gi", "Tuy Phong", "Ham Thuan Bac", "Duc Linh"],
    "Tay Ninh": ["Tay Ninh City", "Go Dau", "Hoa Thanh", "Trang Bang", "Ben Cau"],
    "An Giang": ["Long Xuyen", "Chau Doc", "Tan Chau", "Tri Ton", "Thoai Son"],
    "Dong Thap": ["Cao Lanh", "Sa Dec", "Hong Ngu", "Thanh Binh", "Lai Vung"],
    "Soc Trang": ["Soc Trang City", "Vinh Chau", "Nga Nam", "My Xuyen", "Tran De"],
    "Ca Mau": ["Ca Mau City", "Nam Can", "Dam Doi", "Thoi Binh", "U Minh"],
    "Bac Lieu": ["Bac Lieu City", "Gia Rai", "Phuoc Long", "Dong Hai", "Hoa Binh"],
}

sparkContext = SparkContext.getOrCreate()
spark = SparkSession(sparkContext)
df_purchase = spark.read.csv("./purchases_copy.csv", header=True, inferSchema=True)
df_product = spark.read.csv("./product_cleaned.csv", header=True, inferSchema=True)
df_purchase = df_purchase.drop("regualar_points_received", "express_points_received", "regular_points_spent", "express_points_spent", "trn_sum_from_iss", "trn_sum_from_red")

df_purchase = df_purchase.join(
    df_product.select("product_id", "product_price"),  # chỉ join cột cần thiết
    on="product_id",
    how="left"
).withColumn(
    "purchase_sum", col("product_price") * col("product_quantity")
).drop("product_price")  # bỏ product_price nếu không cần

df_purchase.show(50, truncate=False)


df_purchase.coalesce(1).write.csv(
    "./purchases_cleaned",
    header=True,
    mode="overwrite",
)
df_purchase.printSchema()