from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType, IntegerType , StructField
import pymysql
#Type	Location	Date	CrashesInYear	CrashesInPlace	FailureRate

#conn = pymysql.connect(host=host, port=port, user=username, passwd=password, db=database)
#cursor = conn.cursor()

def insertDB(row):
    # Define the connection details for your PHPMyAdmin database
    host = "localhost"
    port = 3306
    database = "bigdataproj"
    username = "root"
    password = ""
    
    conn = pymysql.connect(host=host, port=port, user=username, passwd=password, db=database)
    cursor = conn.cursor()

    # Extract the required columns from the row
    column1_value = row["Date"]
    column2_value = row["CrashesInYear"]
    column3_value = row["Location"]
    column4_value = row["CrashesInPlace"]
    column5_value = row["Type"]
    column6_value = row["FailureRate"]

    # Prepare the SQL query to insert data into the table
    sql_query = fr"""INSERT INTO output (date,crashesInYear,location,crashesInPlace,type,failureRate) VALUES ("{column1_value}", "{column2_value}", "{column3_value}", "{column4_value}", "{column5_value}", "{column6_value}")"""

    
    # Execute the SQL query
    cursor.execute(sql_query)

    # Commit the changes
    conn.commit()
    conn.close()

# Create a Spark session
spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

# Define the schema for your DataFrame
schema = StructType().add("Type", StringType(),True).add("Location", StringType(),True).add("Date", StringType(),True).add("CrashesInYear", StringType(),True).add("CrashesInPlace", StringType(),True).add("FailureRate", StringType(),True)

# Read data from Kafka topic as a DataFrame
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test") \
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("data"))

# Select specific columns from "data"
df = df.select("data.Date","data.CrashesInYear", "data.Location", "data.CrashesInPlace", "data.Type","data.FailureRate")

# Convert the value column to string and display the result
query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .foreach(insertDB) \
    .start()

# Wait for the query to finish
query.awaitTermination()

