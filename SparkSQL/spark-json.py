from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").\
        appName("jsonapp").\
        getOrCreate()


# file options
multiline = True
file_path = "s3a://datasets-spark-learning/SemiStructured-files/sample.json"

#Read json data from files.directoreies
json_df = spark.read.format("json").option("multiLine",multiline).load(file_path)

#PrintSample data
print(json_df.show(5))

#printthe schema
print(json_df.printSchema())
print(json_df.schema)

spark.stop()

