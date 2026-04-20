from pymongo import MongoClient

from pyspark.sql import functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0") \
    .getOrCreate()

mongo_url = "mongodb://mongodb:27017/amazon_data"

reviews = spark.read.csv("amazon_reviews.csv", header=True, multiLine=True, escape='"')

reviews = reviews.dropna(subset=["review_id", "product_id", "star_rating", "review_date"])
reviews = reviews.withColumn("review_date", F.to_date(F.col("review_date")))
reviews = reviews.withColumn("year", F.year("review_date"))
reviews = reviews.withColumn("month", F.month("review_date"))
reviews = reviews.filter(F.col("verified_purchase") == "1")

product_grade = reviews.groupBy("product_id").agg(
    F.count("product_id").alias("number_of_reviews"),
    F.round(F.avg(F.col("star_rating").cast("int")), 1).alias("average_rating")
)

user_verified_reviews = reviews.groupBy("customer_id").agg(
    F.count("customer_id").alias("number_of_reviews")
)

product_reviews_by_month = reviews.groupBy("product_id", "year", "month").agg(
    F.count("product_id").alias("number_of_reviews")
)

def write_product_grade():
    product_grade \
        .withColumnRenamed("product_id", "_id") \
        .write \
        .format("mongodb") \
        .mode("overwrite") \
        .option("spark.mongodb.write.connection.uri", mongo_url) \
        .option("spark.mongodb.write.collection", "product_ranking") \
        .save()
    
def get_product_grade(product_id):
    df_from_mongo = spark.read \
    .format("mongodb") \
    .option("spark.mongodb.read.connection.uri", mongo_url) \
    .option("spark.mongodb.read.collection", "product_ranking") \
    .load()

    result = df_from_mongo.filter(F.col("_id") == product_id)
    result.show()

def write_user_verified_reviews():
    user_verified_reviews \
        .withColumnRenamed("customer_id", "_id") \
        .write \
        .format("mongodb") \
        .mode("overwrite") \
        .option("spark.mongodb.write.connection.uri", mongo_url) \
        .option("spark.mongodb.write.collection", "user_reviews") \
        .save()
    
def get_user_verified_reviews(user_id):
    df_from_mongo = spark.read \
    .format("mongodb") \
    .option("spark.mongodb.read.connection.uri", mongo_url) \
    .option("spark.mongodb.read.collection", "user_reviews") \
    .load()

    result = df_from_mongo.filter(
        (F.col("_id") == user_id) 
    )
    result.show()

def write_product_reviews_by_month():
    product_reviews_by_month.write \
        .format("mongodb") \
        .mode("overwrite") \
        .option("spark.mongodb.write.connection.uri", mongo_url) \
        .option("spark.mongodb.write.collection", "product_reviews_by_month") \
        .save()
    
    
    client = MongoClient("mongodb://mongodb:27017/")
    db = client["amazon_data"]
    
    collection = db["product_reviews_by_month"]
    
    collection.create_index([("product_id", 1)])
        
def get_product_reviews_by_month(product_id, year, month):
    df_from_mongo = spark.read \
    .format("mongodb") \
    .option("spark.mongodb.read.connection.uri", mongo_url) \
    .option("spark.mongodb.read.collection", "product_reviews_by_month") \
    .load()

    result = df_from_mongo.filter(
        (F.col("product_id") == product_id) &
        (F.col("year") == year) &
        (F.col("month") == month)
    )
    result.show()

# write_product_grade()
# get_product_grade("0842329129")
# write_user_verified_reviews()
# get_user_verified_reviews("52496855")
# write_product_reviews_by_month()
# get_product_reviews_by_month("0312336853", "2005", "10")