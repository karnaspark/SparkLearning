import pyspark

from pyspark.sql import SparkSession

#Creation of spark session object with the customised configurations
spark = SparkSession.builder.\
        master("yarn").\
        appName("spark_dataframe_csv").\
        getOrCreate()

#Data  paths
hdfs_host = "ip-172-31-0-217"
s3_file_path = "s3a://datasets-spark-learning/flat_files/ca-500.csv"
hdfs_file_path = f"hdfs://{hdfs_host}:8020/user/hadoop/au-500.csv"
local_file_path = "file:///home/hadoop/sample.csv"

#File format parametres
file_format = "csv"
header = True

#Extract data from csv files from above storage systems into Spark as dataframes
#Extracting from HDFS
print("Extracting data from HDFS")
hdfs_df = spark.read.format(file_format).option("header",header).load(hdfs_file_path)
print("sample hdfs dataframe is" )
print(hdfs_df.show(10))

#Extracting from s3
print("Extracting data from S3")
s3_df = spark.read.format(file_format).option("header",header).load(s3_file_path)
print("sample s3 dataframe is" )
print(s3_df.show(10))

#extracting from LFS
# print("Extracting data from LFS")
# local_df = spark.read.format(file_format).option("header",header).load(local_file_path)
# print("sample lfs dataframe is" )
# print(local_df.show(10))

#Stop the spark application
spark.stop()
