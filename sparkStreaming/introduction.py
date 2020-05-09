"""
Spark Streaming

Near Real time
with minimum latency of time gap ( with a minimum possible latency of 1 sec)

Spark core --> Spark context-->RDD
Spark SL --> SQLcontext, hivecontext, sparksession --> Dataframes
Spark Streaming --> StreamingContext --> Dstreams Discretized Streams
Structured Streaming --> SparkSession (from spark 2.x onwards)
"""
from pyspark.sql import SQLcontext,HiveContext
sqlc = SQLContext(sc)
hivec = HiveContext(sc)

from pyspark.streaming import StreamingContext
 #Streaming context can be created out of SparkContext object, we also need to pass the time latency 
streamc = StreamingContext(sc,batchDuration=30) 

# connect to the source
ds1 = streamc.socketTextStream("localhost",2345)

#Transformation and actions on the data
ds2 = ds1.flatMap(lambda x:x.split(" ")).map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y)
ds2.saveAsTextFiles("file:///home/hadoop/streaming/stream-out")
ds2.pprint()   

#Start the streaming application
streamc.start()

streamc.stop(False,True)




# 20/05/09 01:47:00 WARN StreamingContext: spark.master should be set as local[n], n > 1 in local mode if you have receivers to get data,
#  otherwise Spark jobs will not get resources to process the received data.
# 20/05/09 01:54:45 WARN RandomBlockReplicationPolicy: Expecting 1 replicas with only 0 peer/s.

master local[1] --> WARN, no processing ,only receiving
# 20/05/09 01:47:00 WARN StreamingContext: spark.master should be set as local[n], n > 1 in local mode if you have receivers to get data,
#  otherwise Spark jobs will not get resources to process the received data.
# 20/05/09 01:54:45 WARN RandomBlockReplicationPolicy: Expecting 1 replicas with only 0 peer/s.
Sspark streaming app can receive data from multiple sources
For each source, A receiver is needed 
In local mode, atleast we need to provide (number of receivers + 1 ) no of cores
2 sources, 2 receivers --> 2+1 ->3 cores


master local[2] --> No WARN, receivieng, processing
1 core --> receiving
1 core --> processing


Graceful shutdown
batchDuration =1 min --> processing time
7.00 --7.01 -->P1(b1)   -> microbatching
7.01 --7.02 -->P(b2)
7.02 --7.03 -->P(b3)
7.03 --7.04 -->P(b4)
7.04 ---->7.04.30 --> stopeed the application-->
            if we stop the applciation abnormally, we lose some data without processing
            if we stop the applciation gracefully, spark make 2 more attempts to process the already recived before issuing the stop instruction

streamc.stop(True,False)<---> streamc.stop()--> Default abnormal termination 
streamc.stop(False,True)--> Graceful
        1 st arguemnt --> do you want to stop also the underlying spark context -->
        2 d arguemnt --> stop the application gracefully --> it will process the data with 2 more processing attempts.


