from cassandra.cluster import Cluster
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
import os


def create_cluster():
    cassandra_host = os.getenv("CASSANDRA_HOST", "127.0.0.1")
    cluster = Cluster([cassandra_host], port=9042)

    return cluster

def create_tables():
    cluster = create_cluster()
    session = cluster.connect() 

    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS amazon 
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
    """)

    session.set_keyspace('amazon')

    session.execute("""
        CREATE TABLE IF NOT EXISTS product_reviews (
            product_id text,
            review_id text,
            star_rating int,
            review_headline text,
            review_body text,
            PRIMARY KEY ((product_id), star_rating, review_id)
        );
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS customer_reviews (
            product_id text,
            review_id text,
            star_rating int,
            review_headline text,
            review_body text,
            customer_id text,
            review_date date,
            PRIMARY KEY ((customer_id), review_date, review_id)
        );
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS most_reviewed_items_for_period (
            review_date date,
            product_id text,
            review_numbers int,
            PRIMARY KEY ((review_date), product_id)
        );
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS most_productive_customers_for_period (
            review_date date,
            customer_id text,
            reviews_count int,
            hater_reviews_count int,
            backer_reviews_count int,
            PRIMARY KEY ((review_date), customer_id)
        );
    """)

    cluster.shutdown()

def extract_data_from_csv():
    cassandra_host = os.getenv("CASSANDRA_HOST", "127.0.0.1")
    spark = SparkSession.builder \
        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1") \
        .config("spark.cassandra.connection.host", cassandra_host) \
        .getOrCreate()


    reviews = spark.read.csv("amazon_reviews.csv", header=True, multiLine=True, escape='"')

    reviews = reviews.withColumn("review_date", F.to_date(F.col("review_date")))
    reviews = reviews.withColumn("star_rating", F.col("star_rating").cast("int"))
    reviews = reviews.dropna(subset=["review_id", "product_id", "star_rating", "review_date"])
    reviews = reviews.withColumn("year", F.year("review_date"))
    reviews = reviews.withColumn("month", F.month("review_date"))
    reviews = reviews.filter(F.col("verified_purchase") == "1")

    return reviews

def load_to_product_reviews_table():
    product_reviews = extract_data_from_csv().select(
        "product_id",
        "review_id",
        "star_rating",
        "review_headline",
        "review_body"
    )

    product_reviews.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="product_reviews", keyspace="amazon") \
        .mode("append") \
        .save()

def load_to_customer_reviews_table():
    customer_reviews = extract_data_from_csv().select(
        "product_id",
        "review_id",
        "star_rating",
        "review_headline",
        "review_body",
        "customer_id",
        "review_date"
    )

    customer_reviews.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="customer_reviews", keyspace="amazon") \
        .mode("append") \
        .save()

create_tables()

def load_to_most_reviewed_items_for_period():
    data = extract_data_from_csv().groupBy("review_date", "product_id").agg(
        F.count("product_id").alias("review_numbers")
    )

    data.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="most_reviewed_items_for_period", keyspace="amazon") \
        .mode("append") \
        .save()
    

def load_to_most_productive_customers_for_period():
    data = extract_data_from_csv().groupBy("review_date", "customer_id").agg(
        F.count("customer_id").alias("reviews_count"),
        F.count(
            F.when((F.col("star_rating") <= 2), 1)
        ).alias("hater_reviews_count"),
        F.count(
            F.when((F.col("star_rating") > 3), 1)
        ).alias("backer_reviews_count")
    )

    data.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="most_productive_customers_for_period", keyspace="amazon") \
        .mode("append") \
        .save()
    
load_to_most_productive_customers_for_period()
load_to_most_reviewed_items_for_period()
load_to_customer_reviews_table()
load_to_product_reviews_table()

