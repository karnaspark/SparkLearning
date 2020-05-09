from pyspark import SparkContext,SparkConf
from pyspark.streaming import StreamingContext

conf = SparkConf()
conf.set("spark.master","local[2]")
conf.set("spark.app.name","streamingapp")

sc = SparkContext(conf=conf)

streamc = StreamingContext(sc,batchDuration=15) 


ds1 = streamc.socketTextStream("localhost",23450)
ds2 = ds1.window(windowDuration = 30).flatMap(lambda x:x.split(" ")).map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y)
ds2.pprint()

streamc.start()
streamc.awaitTermination()
