from pyspark.sql import SparkSession, Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import *
from lib.logger import Log4j
# import pyspark

spark = SparkSession \
    .builder \
    .master('local') \
    .appName('ASM1') \
    .config('spark.mongodb.read.connection.uri', 'mongodb://admin:123@localhost:27017/DEP303_ASM1.question?authSource=admin') \
    .getOrCreate()

# 2. ĐỌC DỮ LIỆU TỪ MONGODB
# questionDF = spark.read \
#     .format('mongodb') \
#     .option("inferSchema", "true") \
#     .load()
#
# questionDF.printSchema()
# questionDF.show()

# 3. CHUẨN HÓA LẠI DỮ LIỆU: NGÀY THÁNG, NULL

# new_questionDF = questionDF \
#     .withColumn("OwnerUserId", when(col("OwnerUserId") == "NA", None).otherwise(col("OwnerUserId"))) \
#     .withColumn("ClosedDate", when(col("ClosedDate") == "NA", None).otherwise(col("ClosedDate"))) \
#     .select("ID",
#             col("OwnerUserId").cast("int"),
#             regexp_extract("CreationDate", "(.+)T", 1).alias("CreationDate"),
#             regexp_extract("ClosedDate", "(.+)T", 1).alias("ClosedDate"),
#             "Score",
#             "Title",
#             "Body"
#             ) \
#     .withColumn("CreationDate", to_date("CreationDate", "yyyy-MM-dd")) \
#     .withColumn("ClosedDate", to_date("ClosedDate", "yyyy-MM-dd"))

# new_questionDF.printSchema()
# new_questionDF.show()

# 4. THỐNG KÊ SỐ NGÔN NGỮ ĐƯỢC HỎI, SỬ DỤNG REGEX ĐỂ LỌC RA CÁC NGÔN NGỮ

# new_questionDF \
#     .select(regexp_extract("Body", r"Java|Python|C\+\+|C#|Go|Ruby|Javascript|PHP|HTML|CSS|SQL", 0).alias(
#     "Programing Language")) \
#     .withColumn("Programing Language", when(col("Programing Language") == "", None).otherwise(col("Programing Language"))) \
#     .filter(col("Programing Language").isNotNull()) \
#     .groupBy("Programing Language") \
#     .agg(count("*").alias("Count")) \
#     .show()

# 5. TÌM CÁC DOMAIN ĐƯỢC SỬ DỤNG NHIỀU NHẤT TRONG CÁC CÂU HỎI

# pattern = r"http.+?\/\/(.+?)\/"
# new_questionDF \
#     .select(regexp_extract("Body", pattern, 1).alias("Domain")) \
#     .withColumn("Domain", when(col("Domain") == "", None).otherwise(col("Domain"))) \
#     .filter(col("Domain").isNotNull()) \
#     .groupBy("Domain") \
#     .agg(count("*").alias("Count")) \
#     .sort(col("Count").desc()) \
#     .show()

# 6. TÍNH TỔNG ĐIỂM CỦA USER THEO TỪNG NGÀY

# running_total_window = Window.partitionBy("OwnerUserId") \
#     .orderBy("CreationDate") \
#     .rowsBetween(Window.unboundedPreceding, Window.currentRow)
#
# new_questionDF \
#     .filter(col("OwnerUserId").isNotNull()) \
#     .withColumn("Score", sum("Score").over(running_total_window)) \
#     .select("OwnerUserId", "CreationDate", "Score") \
#     .show()

# 7. Tính tổng số điểm mà User đạt được trong một khoảng thời gian

# START = '2008-01-01'
# END = '2009-01-01'
#
# new_questionDF \
#     .filter(col("CreationDate").between(START, END)) \
#     .sort(col("OwnerUserId")) \
#     .show()

# 8. TÌM CÁC CÂU HỎI CÓ NHIỀU CÂU TRẢ LỜI
'''
"-- Đọc dữ liệu Answers từ MongoDB"
answersDF = spark.read \
    .format("mongodb") \
    .option("inferSchema", "true") \
    .option("spark.mongodb.read.connection.uri", "mongodb://admin:123@localhost:27017/DEP303_ASM1.answers?authSource=admin") \
    .load()

answersDF.printSchema()
answersDF.show()

new_questionDF.coalesce(1).write \
    .bucketBy(3, "id") \
    .mode("overwrite") \
    .saveAsTable("question")

answersDF.coalesce(1).write \
    .bucketBy(3, "id") \
    .mode("overwrite") \
    .saveAsTable("my_db.answers")
'''
"-- Thiết lập 3 phân vùng song song"
spark.conf.set("spark.sql.shuffle.partitions", 3)

"-- Đọc dữ liệu từ bucket"
questionDF_bucket = spark.read.parquet("spark-warehouse/question/")
answersDF_bucket = spark.read.parquet("spark-warehouse/answers/")

"-- Lựa chọn cột phù hợp"
new_questionDF = questionDF_bucket.select("Id", "CreationDate", "Title", "Body")
new_answersDF = answersDF_bucket.select("ParentId")

"-- Điều kiện join và join 2 DF"
join_expr = new_questionDF.Id == new_answersDF.ParentId
new_answersDF.join(new_questionDF, join_expr, "inner") \
    .groupBy("Id", "CreationDate", "Title", "Body") \
    .agg(count("*").alias("Count")) \
    .filter("Count > 5") \
    .show()

# 9. YÊU CẦU 6: TÌM CÁC ACTIVE USER
#
# # "-- Thiết lập 3 phân vùng song song"
# spark.conf.set("spark.sql.shuffle.partitions", 3)
#
# # "-- Đọc dữ liệu từ bucket"
# questionDF_bucket = spark.read.parquet("spark-warehouse/question/")
# answersDF_bucket = spark.read.parquet("spark-warehouse/answers/")
#
# # "-- Lựa chọn cột phù hợp"
# new_questionDF = questionDF_bucket.select("Id", "CreationDate")
# new_answersDF = answersDF_bucket.select("OwnerUserId", "ParentId", "Score")
#
# # "-- Điều kiện join và join 2 DF"
# join_expr = new_questionDF.Id == new_answersDF.ParentId
# join_df = new_answersDF.join(new_questionDF, join_expr, "inner")
#
# # "--Tạo view để sử dụng SQL"
# join_df.createTempView("join_table")
# # "-- Dùng sub-query trong WHERE thỏa mãn yêu cầu"
# spark.sql("""
#         SELECT DISTINCT OwnerUserId as ActiveUser
#         FROM join_table
#         WHERE OwnerUserId IN (
#             SELECT OwnerUserId
#             FROM join_table
#             GROUP BY OwnerUserId, CreationDate
#             HAVING count(*) > 5 )
#             OR OwnerUserId IN (
#             SELECT OwnerUserId
#             FROM join_table
#             GROUP BY OwnerUserId
#             HAVING count(*) > 50 OR sum(Score) > 500 )
#         """) \
#     .show()