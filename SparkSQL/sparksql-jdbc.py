from pyspark.sql import SparkSession

#Creation of spark driver 
spark = SparkSession.builder.\
        config("spark.master","local[3]").\
        config("spark.app.name","sparksql-jdbc").\
        config("spark.jars.packages","org.postgresql:postgresql:42.2.12").\
        getOrCreate()

#JDBC source parameters ,the source here is "Postgresql"
host_name = "pyspark-postgresql-database.c7w3ggnyjoqn.us-east-2.rds.amazonaws.com"
port_number = 5432
database = "postgres"
connection_url = f"jdbc:postgresql://{host_name}:{port_number}/{database}"
driver = "org.postgresql.Driver"
user_name = "postgres"
password = "postgresql123"
source_table = "public.samplecsv" 
target_table = "public.samplecsv"
query = "SELECT * FROM public.samplecsv WHERE city = 'Leith'"

#Extract data from postgresql server 
psql_df = spark.read.format("jdbc").\
            option("url",connection_url).\
            option("driver",driver).\
            option("user",user_name).\
            option("password",password).\
            option("query",query).\
            load()
            # option("dbtable",source_table).\
            # load()

#Print sample data
psql_df.show(10)

#Extract data from s3 csv files
csv_df = spark.read.format("csv").option("header",True).option("inferSchema",True).load("s3a://datasets-spark-learning/flat_files/au-500.csv")

print("Sample rows of csv dataframe is..")
print(csv_df.show(5))

#:Load dataframe as atable into psotgresql server
try:
    csv_df.write.format("jdbc").\
                option("url",connection_url).\
                option("driver",driver).\
                option("user",user_name).\
                option("password",password).\
                option("dbtable",target_table).\
                mode("append").\
                save()
except Exception as e:
    print(f"Issue while loading into Postgresql server {e}")
else:
    print("Succesfully loaded data to Postgresql Server")    

#stop spark app
spark.stop()