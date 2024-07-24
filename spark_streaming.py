from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, window, count
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

# Create SparkSession
spark = SparkSession.builder \
    .appName("TwitterStreamAnalysis") \
    .getOrCreate()

# Define schema for the tweets
schema = StructType([
    StructField("text", StringType(), True),
    StructField("user", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("hashtags", ArrayType(StringType()), True)
])

# Read from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "twitter_topic") \
    .load()

# Parse JSON data
parsed_df = df.select(
    col("timestamp").cast("timestamp"),
    col("value").cast("string")
).select(
    col("timestamp"),
    from_json(col("value"), schema).alias("tweet")
)

# Extract hashtags
hashtags_df = parsed_df.select(
    col("timestamp"),
    explode(col("tweet.hashtags")).alias("hashtag")
)

# Count hashtags in 5-minute windows
windowed_counts = hashtags_df \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window(col("timestamp"), "5 minutes", "1 minute"),
        col("hashtag")
    ) \
    .agg(count("*").alias("count"))

# Sort by count in descending order
sorted_counts = windowed_counts.orderBy(col("window"), col("count").desc())

# Start the query to display results
query = sorted_counts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
