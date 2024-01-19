from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

DATA_PATH = "C:\\Users\\Ahmed\\OneDrive\\Desktop\\SingleTable\\datasets"



# Create a Spark session
spark = SparkSession.builder \
    .appName("PySparkProducer") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

#EXTRACT STAGE 

# Define the schema for the DataFrame
datasetSchema = StructType().add("Date", StringType(),True).add("Location", StringType(),True).add("Operator",StringType(),True).add("Type", StringType(),True).add("Aboard", StringType(),True).add("Fatalities", StringType(),True)

# Read data from a directory as a streaming DataFrame
initialDF = spark.readStream.format("csv").schema(datasetSchema).option("header","true").option("path", DATA_PATH).load()

#TRANSFORMATION STAGE (At Producer)
columnsToFill = {
    "Location": mode(col("Location")),
    "Operator": mode(col("Operator")),
    "Type": mode(col("Type")),
    "Aboard": mean(col("Aboard")),
    "Fatalities": mean(col("Fatalities"))
}

finalDF = initialDF.fillna('Location', subset=['Location']).fillna('Operator', subset=['Operator']).fillna('Type', subset=['Type']).fillna('Aboard', subset=['Aboard']).fillna('Fatalities', subset=['Fatalities'])

cols = finalDF.select("Date","Location", "Operator","Type", "Aboard","Fatalities")
# Convert the selected columns to JSON and alias the column as "value"
streamingDF = finalDF.select(to_json(struct("Date","Location", "Operator","Type", "Aboard","Fatalities")).alias("value"))

# Convert the value column to string and display the result
query = streamingDF.selectExpr("CAST(value AS STRING)") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "test") \
    .option("checkpointLocation", "null") \
    .start()

# Wait for the query to finish
query.awaitTermination()

