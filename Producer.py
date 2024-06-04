from pyspark.sql import SparkSession,functions as f,types
from datetime import datetime
import os

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'airplanecrashes'

DATA_PATH = "C:\\Users\\Ahmed\\OneDrive\\Desktop\\AirplaneCrashes\\Datasets"
PROC_DATA_PATH = "C:\\Users\\Ahmed\\OneDrive\\Desktop\\AirplaneCrashes\\ProcessedDS"

CURRENT_DATE = datetime.today().strftime('%d-%m-%Y')  
CURRENT_TIME = datetime.now().strftime('%H:%M:%S')  

LOG_DIR = "C:\\Users\\Ahmed\\OneDrive\\Desktop\\AirplaneCrashes\\Logs"
LOG_NAME = f"{CURRENT_DATE}-Log.txt"

CURR_LOG_FILE = os.path.join(LOG_DIR, LOG_NAME)

spark = SparkSession.builder \
    .appName("PySparkProducer") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

#Extracting the Data from the Dataset residing in the datasets directory, automatically inferring the schema using the header option
df = spark.read.format("csv").option("header","true").load(DATA_PATH)

print("Data read from source.")

#Picking the columns we wish to work with, dropping the rest (Damaged data, too many null values in the other columns)
FilteredDF = df.select("Date","Location", "Operator","Type", "Aboard","Fatalities")

#First Transformation Stage 
TransformedDF = FilteredDF\
    .withColumn("ID",f.monotonically_increasing_id())\
    .withColumn("Aboard",f.col("Aboard").astype("int"))\
    .withColumn("Fatalities",f.col("Fatalities").astype("int"))\
    .select("ID","Date","Location", "Operator","Type", "Aboard","Fatalities")

#Preparing a list to store the columns that contain null values, an object to store it in the form of {ColName:[Row IDs of null values]}
NullCols = []
NullColsData = {}
for col in TransformedDF.columns:
    #Filtering to keep columns that contain NULL Values
    if TransformedDF.select(f.col("ID"), col).filter(f.col(col).isNull()).count() > 0:
        NullCols.append(col)
        for nullCol in NullCols:
            #Adding Row IDs to a list and then adding it to the corresponding col name as Key
            nullColsID = [row['ID'] for row in TransformedDF.select(f.col("ID")).filter(f.col(nullCol).isNull()).collect()]
            NullColsData[nullCol] = nullColsID
    
#First Stage of Logging
with open(CURR_LOG_FILE,"w") as file:
    for key, value in NullColsData.items():
        file.write(f"{CURRENT_TIME} - ALERT - Column {key} has NULL Values, Row IDs: {value}\n")        
        

print("Log File Created, Information about Data before transformation written to it.")

#Logging after Transformation
TIME_AFTER_TRANSFORMATION = datetime.now().strftime('%H:%M:%S') 

#Second Stage of Transformation (There are many ways to handle NULL Values, but looking at the fact that this is historical data, 
#NULL Values cannot be handled Using traditional ways as this would be historically inaccurate)
#However, Issues relating to data types of columns have been handled
FinalDF = TransformedDF.dropna()

with open(CURR_LOG_FILE, "a") as file:
    file.write(f"{TIME_AFTER_TRANSFORMATION} - SUCCESS - Data Transformed Successfully!")

print("Information about Data after transformation appended to file successfully.")

FinalDF.write.format("csv").option("header","true").mode("overwrite").save(PROC_DATA_PATH)

print(f"Transformed Dataset saved to {PROC_DATA_PATH}")

spark.stop()


