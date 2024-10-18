from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as f
from InitLogging import Initialize
from InitSparkSession import initSpark

logger = Initialize()

def Extract(filepath: str, format:str, options:dict = None) -> DataFrame:
    """This function extracts the data from different flat file sources
    
    filepath(str): The file path where the flat file resides
    format(str): The format of the flat file (csv,json etc..)
    options(dict): The options you wish to enforce when ingesting the file (inferSchema, Header)"""
    spark = initSpark()
    df = None  
    if format in ['csv', 'json', 'avro', 'parquet']:
        try:
            df = spark.read.format(format).options(**options).load(filepath)
            logger.info(f"EXTRACT - File extracted from source succssfully, file format: {format}, filepath: {filepath}")
            print("File Extracted from source successfully")
        except FileNotFoundError:
            logger.error(f"EXTRACT - File not found: {filepath}")
            print("File you're trying to extract is not found, please check the path and try again.")
        except Exception as e:
            logger.error(f"EXTRACT - Error reading the file: {e}")
            print("An Error occured while trying to read the file.")
    else:
        logger.error(f"EXTRACT - Unsupported file format: {format}")
        print("The file format you're trying to read is not supported.")
    return df  

