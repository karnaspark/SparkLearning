from pyspark.sql import SparkSession


#Creation of spark driver 
#Enable hive support when needs to connect to hive 
spark = SparkSession.builder.\
        config("spark.master","local[3]").\
        config("spark.app.name","spark-hive").\
        enableHiveSupport().\
        getOrCreate()


