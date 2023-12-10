from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StringType, IntegerType

#Type	Location	Date	CrashesInYear	CrashesInPlace	FailureRate

# Create a Spark session
spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

# Define the schema for your DataFrame
schema = StructType().add("Type", StringType(),True).add("Location", StringType(),True).add("Date", StringType(),True).add("CrashesInYear", StringType(),True).add("CrashesInPlace", StringType(),True).add("FailureRate", StringType(),True)

# Read data from a directory as a streaming DataFrame
streamDf = spark.readStream.format("csv").schema(schema).option("header","true").option("path", "C:\\Users\\Ahmed\\OneDrive\\Desktop\\SingleTable\\datasets").load()

# Select specific columns from "data"
cols = streamDf.select("Type","Location", "Date","CrashesInYear", "CrashesInPlace","FailureRate")

# Convert the selected columns to JSON and alias the column as "value"
df = streamDf.select(to_json(struct("Type","Location", "Date","CrashesInYear", "CrashesInPlace","FailureRate")).alias("value"))

# Convert the value column to string and display the result
query = df.selectExpr("CAST(value AS STRING)") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "test") \
    .option("checkpointLocation", "null") \
    .start()

# Wait for the query to finish
query.awaitTermination()
