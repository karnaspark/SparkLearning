from pyspark import SparkContext,SparkConf
from pyspark.streaming import StreamingContext

conf = SparkConf()
conf.set("spark.master","local[2]")
conf.set("spark.app.name","streamingapp")

sc = SparkContext(conf=conf)

streamc = StreamingContext(sc,batchDuration=30) 

 #Streaming context can be created out of SparkContext object, we also need to pass the time latency 

ds1 = streamc.textFileStream("file:///home/hadoop/streamdata")
# ds1 = streamc.textFileStream("hdfs://172.31.7.242:8020/user/hadoop/streamdata")
# ds1 = streamc.textFileStream("s3://datasets-spark-learning/flat_files/csvfiles/")
ds1.count()
ds1.pprint()

streamc.start()
streamc.awaitTermination()