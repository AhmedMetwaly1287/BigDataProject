from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import IntegerType, FloatType, StringType
import pyspark.sql.functions as f
from InitLogging import Initialize
from InitSparkSession import initSpark

logger = Initialize()

spark = initSpark()

def Transform(df: DataFrame) -> DataFrame:
    """This function will be used to transform the dataset as needed, this usage of this function will differentiate depending on the 
    dataset/database loaded, this function is especially tailored for the dataset currently in use.
    
    df(DataFrame): The dataframe to be transformed."""
    
    if df == None:
        logger.error("TRANSFORM - DataFrame is invalid/Invalid operation")
        raise ValueError("DataFrame is invalid/Invalid operation")
    else:
        try:
            ProcessedDF = df.drop("Summary")\
                .fillna("Not Found", subset=["Location","Route","Type","Operator","Time","Flight #","Registration","cn/In"])\
                .withColumn("Aboard",df["Aboard"].cast(IntegerType()))\
                .withColumn("Fatalities",df["Fatalities"].cast(IntegerType()))\
                .withColumn("Date",f.to_date("Date","MM/dd/yyyy"))\
                .withColumn("ID",f.monotonically_increasing_id())\
                .select(*["ID","Date","Time","Location","Operator","Flight #","Route","Type","Registration","cn/In","Aboard","Fatalities","Ground"])
            logger.info("TRANSFORM - Dataset transformed successfully")
        except Exception as e:
            logger.error(f"TRANSFORM - An error occured while transforming the dataframe: {e}")
            print(f"An error occured while transforming the dataframe: {e}")
        else:
            return ProcessedDF
    
    