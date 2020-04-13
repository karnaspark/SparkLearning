from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").\
        appName("xmlapp").\
        config("spark.jars","file:///home/hadoop/spark-xml_2.11-0.9.0.jar").\
        getOrCreate()

#File options
tag_name = "record"
file_path = "s3a://datasets-spark-learning/SemiStructured-files/sample.xml"

#Ecxtract xml data
xml_df = spark.read.format("xml").\
            option("rowTag",tag_name).\
            load(file_path)

print("The sample data is")
print(xml_df.show()) #by default show prints top 20 rows 

print("The schema is ")
print(xml_df.printSchema())
print(xml_df.schema)

#stop the appkication
spark.stop()
