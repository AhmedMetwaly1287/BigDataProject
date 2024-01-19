from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pymysql

#Establishing Connection with MySQL database
def establishConnection():
    host = "localhost"
    port = 3306
    database = "bigdataproj"
    username = "root"
    password = ""
    
    conn = pymysql.connect(host=host, port=port, user=username, passwd=password, db=database)
    return conn

#Creating new tables from the existing data in the 'output' table
def createNewTables(query):
    conn = establishConnection()
    cursor = conn.cursor()
    q = query
    cursor.execute(q)
    conn.commit()
    conn.close()

#Inserting data into the database (needs rework)
def insertDB(row):

    conn = establishConnection()
    cursor = conn.cursor()

    # Extract the required columns from the row
    column1_value = row["Date"]
    column2_value = row["Location"]
    column3_value = row["Operator"]
    column4_value = row["Type"]
    column5_value = row["Aboard"]
    column6_value = row["Fatalities"]

    # Prepare the SQL query to insert data into the table
    sql_query = fr"""INSERT INTO output (date,location,operator,type,aboard,fatalities) VALUES ("{column1_value}", "{column2_value}", "{column3_value}", "{column4_value}", "{column5_value}", "{column6_value}")"""
    
    # Execute the SQL query
    cursor.execute(sql_query)

    # Commit the changes
    conn.commit()
    conn.close()
    

def transformData(dataf):
    transformedDF = dataf\
    .withColumn("Location", split("Location", ",").getItem(1))\
    .withColumn("Date", substring(dataf["Date"], 7, 4))\
    .dropna(how="any")
  
    return transformedDF

# Create a Spark session
spark = SparkSession.builder \
    .appName("PySparkConsumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

# Define the schema for your DataFrame
datasetSchema = StructType().add("Date", StringType(),True).add("Location", StringType(),True).add("Operator",StringType(),True).add("Type", StringType(),True).add("Aboard", StringType(),True).add("Fatalities", StringType(),True)

# Read data from Kafka topic as a DataFrame
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test") \
    .load() \
    .select(from_json(col("value").cast("string"), datasetSchema).alias("data"))

df = df.select("data.Date", "data.Location", "data.Operator", "data.Type","data.Aboard","data.Fatalities")

#TRANSFORMATION STAGE
finalDF = transformData(df)

#LOADING STAGE
# Writing the data to the database
query = finalDF.writeStream \
    .outputMode("append") \
    .format("console") \
    .foreach(insertDB) \
    .start()



# Wait for the query to finish after 3 minutes then create the new tables
query.awaitTermination(180)

createNewTables(fr"CREATE TABLE crashesinyear as select distinct(date),count(*) as crashesInYear from output group by date;")
createNewTables(fr"CREATE TABLE crashesinplace as select distinct(location),count(*) as crashesInPlace from output group by location;")
createNewTables(fr"CREATE TABLE failurerate as select distinct(type),count(*) as failureRate from output group by type;")





