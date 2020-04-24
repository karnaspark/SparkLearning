# DataFrame to RDD
df1 = spark.read.format("avro").load("file:///home/hadoop/userdata1.avro")

# Create rdd from a dataframe
r1 = df1.rdd   #Create a Row RDD out of this dataframe

# The RDD created will be called as a Row RDD and we can again create dataframe from this RDD using below statements
schema = df1.schema
df3 = spark.createDataFrame(rdd1,schema)

#create RDD from some file
sparkcontext is needed to create RDD
sc.textFile("s3a://datasets-spark-learning/flat_files/au-500.csv")
rdd3 = rdd2.map(lambda rec:rec.split(",")).map(lambda x:(x[0],x[1]))


# we can create dataframe from an RDD where erecords of RDD are tuples
rdd.toDF()
rdd3.toDF(["firstname","lastname"]).show(5)  #Pass column names 

# Create empty data frame
    # Create an empty RDD and schema 
    no direct fucntion to create empty dataframe,but we ahve a fucntion to create empty rdd
    erdd = sc.emptyRDD()
    df5 = spark.createDataFrame(erdd,custom_schema)