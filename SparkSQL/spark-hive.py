from pyspark.sql import SparkSession


#Creation of spark driver 
#Enable hive support when needs to connect to hive 
spark = SparkSession.builder.\
        config("spark.master","local[3]").\
        config("spark.app.name","spark-hive").\
        enableHiveSupport().\
        getOrCreate()

#We can work with hive data in 2 ways
#1. Either we can create a dataframe out of a hive table
#2. We can run sql on hive tables.
