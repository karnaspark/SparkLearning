

Spark
# temporary and permanant tables
# permanat tables --> managed and external tables

dataframe --> Cannot apply ANSI SQL statements, spark sql API
table --> ANSI SQL 

df1.registerTempTable("csv_spark_table") #deprecated from spark 2.x onwards
df1.createOrReplaceTempView("csv_spark_table")

dataframe --> is it temporary or permanant?

#External tables creation from spark
#If path is specified , tha table will be created as external table
spark.catalog.createTable(path = "/user/hive/warehouse/part_table/",tableName="ext_table")
spark.catalog.createTable(path = "s3a://datasets=spark-learning/flat_files/au-500.csv",tableName="ext_csv_table",source="csv",**{"header":"true","inferschema":"true"})

#Create managed tables
# we dont specify the path, hence schema parsing is not possible, we need to provide the schema
#Create schema in Spark SQL
from pyspark.sql.types import *
custom_schema = STructType([StructField("c1",IntegerType()),StructField("c2",StringType())])
spark.catalog.createTable(tableName = "spark_managed_table",schema = custom_schema )


#Create dataframe from hive table
spark.read.table("database.table")

#Create table in give from spark df
df.write.format("format").saveAsTable("database.table_name")

#Access hive data remotely from HiveServer2
"""
https://dwgeek.com/guide-connecting-hiveserver2-using-python-pyhive.html/

sudo apt-get install libsasl2-dev
pip install pyHive
pip install sasl
pip install thrift
pip install thrift_sasl

"""
from pyhive  import hive
import pandas as pd
#Pass hostname of the machine where HiveSErver2 is running
cursor = hive.connect('ec2-18-188-247-53.us-east-2.compute.amazonaws.com').cursor()
cursor.execute('SELECT * FROM perm_table LIMIT 10')
# print (cursor.fetchone())
df = pd.DataFrame(cursor.fetchall())
df

#Create datafrmae from a local collection
from datetime import datetime,timedelta

r1 = {"date":datetime.today(),"name":"spark","id":2}
r2 = {"date":datetime.today()+timedelta(1),"name":"spark","id":2}
r3 = {"date":datetime.today()+timedelta(2),"name":"spark","id":2}

local_df = spark.createDataFrame([r1,r2,r3])

r3 = {"date":datetime.today()+timedelta(2),"name":"spark","id":3}

r4 = {"date":datetime.today()+timedelta(3),"name":"spark","id":45}
r5 = {"date":datetime.today()+timedelta(5),"name":"spark","id":5}

local_df3 = spark.createDataFrame([r3,r4,r5])





