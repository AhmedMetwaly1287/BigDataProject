from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

PROC_DATA_PATH = "C:\\Users\\Ahmed\\OneDrive\\Desktop\\AirplaneCrashes\\ProcessedDS"

spark = SparkSession.builder \
    .appName("PySparkConsumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

#Read the data from the Processed Dataset, parse it into a Dataframe
df = spark.read.format("csv").option("header","true").load(PROC_DATA_PATH)

# Inserting Data into MySQL Database
df.write.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/airplanecrashes") \
    .option("dbtable", "output") \
    .option("user", "root") \
    .option("password", "") \
    .mode("overwrite") \
    .save()

print("Data inserted into database successfully")

spark.stop()









