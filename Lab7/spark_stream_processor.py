from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max, min, count

# Initialize Spark
spark = SparkSession.builder \
    .appName("CarPriceStreamingApp") \
    .getOrCreate()

# Read streaming data from the folder
streaming_df = spark.readStream \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("stream_data")

# Compute metrics
metrics_df = streaming_df.groupBy("Brand").agg(
    count("*").alias("Count"),
    avg("Price").alias("Avg_Price"),
    max("Price").alias("Max_Price"),
    min("Price").alias("Min_Price")
)

# Output to console
query = metrics_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
