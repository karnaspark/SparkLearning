from pyspark import SparkContext,SparkConf
from pyspark.streaming import StreamingContext

conf = SparkConf()
conf.set("spark.master","yarn")
conf.set("spark.app.name","streamingapp")

sc = SparkContext(conf=conf)

streamc = StreamingContext(sc,batchDuration=15) 

r1 =sc.textFile("s3://datasets-spark-learning/flat_files/au-500.csv")

ds1 = streamc.textFileStream("s3://datasets-spark-learning/flat_files/csvfiles/")

ds2  =ds1.transform(lambda rdd: rdd.union(r1).map(lambda x:x+"spark"))
ds2.pprint()

streamc.start()
streamc.awaitTermination()

