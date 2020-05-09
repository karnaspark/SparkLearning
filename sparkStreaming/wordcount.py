from pyspark import SparkContext,SparkConf
from pyspark.streaming import StreamingContext

conf = SparkConf()
conf.set("spark.master","local[3]")
conf.set("spark.app.name","streamingapp")

sc = SparkContext(conf=conf)

streamc = StreamingContext(sc,batchDuration=30) 

 #Streaming context can be created out of SparkContext object, we also need to pass the time latency 
streamc = StreamingContext(sc,batchDuration=30) 

# connect to the source
ds1 = streamc.socketTextStream("localhost",2345)

#Transformation and actions on the data
ds2 = ds1.flatMap(lambda x:x.split(" ")).map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y)
ds2.saveAsTextFiles("file:///home/hadoop/streaming/stream-out")
ds2.pprint()   

streamc.start()
#/use this to terminate the job either with any error or any termiante signal from user.
streamc.awaitTermination()
