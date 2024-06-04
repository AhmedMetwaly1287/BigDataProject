from pyspark.sql import SparkSession,functions as f,types
import pymysql
from datetime import datetime
import os

DATA_PATH = "C:\\Users\\Ahmed\\OneDrive\\Desktop\\AirplaneCrashes\\Datasets"

CURRENT_DATE = datetime.today().strftime('%d-%m-%Y')  
CURRENT_TIME = datetime.now().strftime('%H:%M:%S')  

LOG_DIR = "C:\\Users\\Ahmed\\OneDrive\\Desktop\\AirplaneCrashes\\Logs"
LOG_NAME = f"{CURRENT_DATE}-Log.txt"

CURR_LOG_FILE = os.path.join(LOG_DIR, LOG_NAME)

DATABASE_NAME = "airplanecrashes"
DATABASE_TABLE= "output"

spark = SparkSession.builder \
    .appName("SparkETL") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

def ExtractData(path):
    #Extracting the Data from the Dataset residing in the datasets directory, automatically inferring the schema using the header option
    df = spark.read.format("csv").option("header","true").load(DATA_PATH)
    return df

def TransformData(df):
    TransformedDF = df\
    .withColumn("ID",f.monotonically_increasing_id())\
    .withColumn("Aboard",f.col("Aboard").astype("int"))\
    .withColumn("Fatalities",f.col("Fatalities").astype("int"))\
    .select("ID","Date","Location", "Operator","Type", "Aboard","Fatalities")
    return TransformedDF

def LoadData(df,dbName,dbTable):
    df.write.format("jdbc") \
    .option("url", f"jdbc:mysql://localhost:3306/{dbName}") \
    .option("dbtable", f"{dbTable}") \
    .option("user", "root") \
    .option("password", "") \
    .mode("overwrite") \
    .save()



df = ExtractData(DATA_PATH)

print("Data read from source.")

#Picking the columns we wish to work with, dropping the rest (Damaged data, too many null values in the other columns)
FilteredDF = df.select("Date","Location", "Operator","Type", "Aboard","Fatalities")

#First Transformation Stage 
TransformedDF = TransformData(FilteredDF)

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

#Second Stage of Transformation (There are many ways to handle NULL Values, but looking at the fact that this is historical data, 
#NULL Values cannot be handled Using traditional ways as this would be historically inaccurate)
#However, Issues relating to data types of columns have been handled
FinalDF = TransformedDF.dropna()

#Logging after Transformation
TIME_AFTER_TRANSFORMATION = datetime.now().strftime('%H:%M:%S') 

with open(CURR_LOG_FILE, "a") as file:
    file.write(f"{TIME_AFTER_TRANSFORMATION} - SUCCESS - Data Transformed Successfully!")

print("Information about Data after transformation appended to file successfully.")

#Loading the data into the MySQL Database
LoadData(FinalDF,DATABASE_NAME,DATABASE_TABLE)

print("Data inserted into database successfully")

spark.stop()


