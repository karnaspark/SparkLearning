# If the data is not clean, we can take help of regular expressions to parse the data correctly.
import re
def find_pattern(input):
    pattern = re.compile(r"[A-Z]{1}[a-z]{2}\s{1}[A-Z]{1}[a-z]{2}\s{1}\d{2}\s{1}\d{2}:\d{2}:\d{2}\s{1}-0800\s{1}\d{4}")
    res = pattern.match(input).group(0)
    return res


# The json file "ad-events_2014-01-20_00_domU-12-31-39-01-A1-34" in s3 is not formatted correctl. we need to cleanse it
json_rdd = sc.textFile("s3a://datasets-spark-learning/SemiStructured-files/ad-events_2014-01-20_00_domU-12-31-39-01-A1-34")
json_rdd.map(lambda x: find_pattern(x)

"""
Mon Jan 20 00:00:00 -0800 2014 ---> {"timestamp":"Mon Jan 20 00:00:00 -0800 2014"}
find_pattern(x) --> x.replace(find_pattern(x),f'{{"timestamp":"find_pattern(x)"}')
"""
# Approach 1:
json_rdd.map(lambda x:x.replace(find_pattern(x),f'{{"timestamp":"find_pattern(x)"}') )
json_rdd2 = json_rdd.map(lambda x:x.replace(find_pattern(x),f'{{"timestamp":"{find_pattern(x)}"') ).map(lambda x: x.replace(', {"cl"',',"cl"')).map(lambda c: c.replace("}, {", ","))

# Approach 2: Excluding the time stamp, create another rdd-> df from timestamp and join the both after solving the timestamp issue
json_rdd3 = json_rdd.map(lambda x: x.split("2014, ")).map(lambda x: x[1]).map(lambda x: x.replace("}, {",","))
# -------------------------------------------------------------------------------------------------------------------------------------------------------------
#Enforcing Schema
custom_schema = StructType([StructField("firstname",StringType()),StructField("lastname",StringType()),StructField("anonymouus",StringType()),StructFi
eld("Address",StringType()),StructField("city",StringType()),StructField("state",StringType()),StructField("post",IntegerType()),StructField("ph1",String
Type()),StructField("ph2",StringType()),StructField("email",StringType()),StructField("url",StringType())])

header_df = spark.read.format("csv").schema(custom_schema).load("file:///home/hadoop/au-500.csv")
# ----------------------JSON SCHEMA---------------------------------------------------------------------------------------------------------------------------------------
jschema = StructType([StructField("Address",StructType([StructField("city",StringType()),StructField("state",StringType()),StructField("StreetName",StringType())])),\
    StructField("fname",StringType()),\
StructField("update_version",FloatType())
])

#------------------------------------------------------------------------------------------------------------------------------------------------------------
#Create  RDD from dataframe
df.rdd 

#-------------------------------------------------------------------------------------------------------------------------------------------------------------
# https://docs.databricks.com/spark/latest/spark-sql/udf-python.html
# Spark SQL UDF
# Step 1: Create a function in python
# Step 2: Register the fucntio with sparkSQL
# Step 3: Use the function in transformations

#STEP 1
def domain(email_string):
    if ("hotmail" in email_string):
        return "Microsost"
    elif ("gmail" in email_string):
        return "Google"
    else:
        return "Others"     
# STEP 2
getdomain = udf(domain,StringType())  # to use in dataframes
spark.udf.register("sqldomain",domain,StringType()) #to use in sql

# STEP 3
header_df.withColumn("email",getdomain(col("email"))).show(5)         
sql("SELECT *,sqldomain(email) from table1").show(5)        

