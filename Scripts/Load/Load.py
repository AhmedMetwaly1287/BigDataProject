from InitLogging import Initialize
from InitSparkSession import initSpark
from pyspark.sql import DataFrame
import pyspark.sql.functions as f

logger = Initialize()

def Load(df: DataFrame, Database: str, Table: str, mode: str = "append") -> None:
    """This function loads data to different types of databases (MySQL for now)
    
    df(Dataframe): The dataframe you wish to load into the database
    
    Database(str): The name of the database where the table will be created
    
    Table(str): The name of the table that data will be appended/overwritten to
    
    mode(str): How you wish to add the data to the database (append to add it to pre-existing data, overwrite to delete all the data currently
    residing in the database and replace it with the dataframe passed)"""
    spark = initSpark()
    if df == None:
        logger.error("LOAD - Dataframe is either empty, invalid or not found")
        raise ValueError("Dataframe is either empty, invalid or not found")
        
    #Going to add switch cases for different types of databases soon but for now only MySQL is supported
    else:
        df.write.format("jdbc").option("url", f"jdbc:mysql://localhost:3306/{Database}") \
        .option("dbtable", f"{Table}") \
        .option("user", "root") \
        .option("password", "") \
        .mode(f"{mode}")\
        .save()
        logger.info(f"LOAD - Data loaded to destination database successfully, Database:{Database}, Table: {Table} with Mode: {mode}")
        print("Data loaded successfully to the database")
