from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, expr
from pyspark.sql.types import StructType, StructField, StringType

# Create spark session
if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("2 Stream Join") \
        .master("yarn") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.shuffle.partitions", 2) \
        .getOrCreate()

# Define schema for impression data
    impressionSchema = StructType([
        StructField("InventoryID", StringType()),
        StructField("CreatedTime", StringType()),
        StructField("Campaigner", StringType())
    ])

# Define schema for click data
    clickSchema = StructType([
        StructField("InventoryID", StringType()),
        StructField("CreatedTime", StringType())
    ])

# Read stream from kafka topic
    kafka_impression_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "impressions") \
        .option("startingOffsets", "earliest") \
        .load()

# Deserialize and make initial transformation to create impression dataframe
    impressions_df = kafka_impression_df \
        .select(from_json(col("value").cast("string"), impressionSchema).alias("value")) \
        .selectExpr("value.InventoryID as ImpressionID", "value.CreatedTime", "value.Campaigner") \
        .withColumn("ImpressionTime", to_timestamp(col("CreatedTime"), "yyyy-MM-dd HH:mm:ss")) \
        .drop("CreatedTime")

# Read stream from kafka topic
    kafka_click_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "clicks") \
        .option("startingOffsets", "earliest") \
        .load()

# Deserialize and make initial transformation to create click dataframe
    clicks_df = kafka_click_df.select(
        from_json(col("value").cast("string"), clickSchema).alias("value")) \
        .selectExpr("value.InventoryID as ClickID", "value.CreatedTime") \
        .withColumn("ClickTime", to_timestamp(col("CreatedTime"), "yyyy-MM-dd HH:mm:ss")) \
        .drop("CreatedTime")

# Define join conditions
    join_expr = "ImpressionID == ClickID"
    join_type = "inner"

# Join both streaming data
    joined_df = impressions_df.join(clicks_df, expr(join_expr), join_type)

# Write output dataframe to console
    output_query = joined_df.writeStream \
        .format("console") \
        .outputMode("append") \
        .option("checkpointLocation", "Stream/chk-point-dir") \
        .trigger(processingTime="1 minute") \
        .start()

    output_query.awaitTermination()
