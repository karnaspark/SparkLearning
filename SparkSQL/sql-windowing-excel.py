# windowing-Excel_df:

    query = """
        SELECT *,DENSE_RANK() OVER (PARTITION BY email ORDER BY state desc) as wind FROM dftable
        """
    sql(query).show(10)    

    excel_df = spark.read.format("com.crealytics.spark.excel").option("dataAddress", "'EB-2806'!").option("header", "true").load("s3a://datasets-spark-learning/census_data/PCA CDB-2806-F-Census.xlsx")
    
    query1 = """
    SELECT *, ROW_NUMBER() OVER(PARTITION BY TRU ORDER BY TOT_P) AS window FROM excel_table
    """
    sql(query1)

    query2 = """
    SELECT *, DENSE_RANK() OVER(PARTITION BY TRU ORDER BY TOT_P) AS window FROM excel_table
    """
    sql(query2)


    query3 = """
    SELECT *, RANK() OVER(PARTITION BY TRU ORDER BY TOT_P) AS window FROM excel_table
    """
    sql(query3)

# Bucketing-Partitioning
    rn.withColumnRenamed("DT Name","DT_Name").withColumnRenamed("CD Block","CD_Block").write.part
    itionBy("CD_Block","TRU").save("file:///home/hadoop/window2")


    rn2.write.bucketBy(5,"DT_Name").saveAsTable("default.bucket_table")



