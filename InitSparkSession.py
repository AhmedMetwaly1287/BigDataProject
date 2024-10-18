from pyspark.sql import SparkSession

# Declare _spark as a global variable
_spark = None

def initSpark() -> SparkSession:
    """This function initailizes/Retrieves Spark Session globally as to not have to create a separate SparkSession for each part of our
    ETL Script"""
    global _spark  # Declare that we're using the global _spark variable
    if _spark is None:
        _spark = SparkSession.builder.appName("Spark Session").getOrCreate()
    return _spark
