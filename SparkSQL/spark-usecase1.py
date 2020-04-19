#This is about a movie dataset
#We can get the movies datasets here :  http://grouplens.org/datasets/movielens/1m/ 
From the datsets given, 
Provide solutions for the followng questions
1.   Top ten most viewed movies with their movies Name (Ascending or Descending order)  
2.   Top twenty rated movies (Condition: The movie should be rated/viewed by at least 40 users) 
3.    We wish to know how have the genres ranked by Average Rating, for each profession and age group. The age groups to be considered are: 18-35, 36-50 and 50+. 
#   ----------------------------------------------------------------------------------------------------------------------------------
# This section has solutions for the first 2 questions

# Solution 1:
    movies_df1 = spark.read.format("text").load("/user/hadoop/movies.dat")
    movies_df1.select(split("value","::").alias("splitdata")).select(col("splitdata")[0].alias("MovieId"),col("splitdata")[1].alias("Title"),col("splitdata")[2]
    .alias("Genres")).show(5)
    movies_df2 = movies_df1.select(split("value","::").alias("splitdata")).select(col("splitdata")[0].alias("MovieId"),col("splitdata")[1].alias("Title"),col("splitdata")[2].alias("Genres")).withColumn("MovieId",col("MovieId").cast(IntegerType()))
    ratings_df1 = spark.read.format("text").load("/user/hadoop/ratings.dat")
    
    ratings_df2 = ratings_df1.select(split("value","::").alias("splitdata")).\
    select(col("splitdata")[0].alias("UserId"),col("splitdata")[1].alias("MovieId"),col("splitdata")[2].alias("rating"),col("splitdata")[3].alias("Timestamp")).\
    withColumn("MovieId",col("MovieId").cast(IntegerType())).\
    withColumn("UserId",col("UserId").cast(IntegerType())).\
    withColumn("rating",col("rating").cast(FloatType()))

# Join the dataframes
    movies_ratings = movies_df2.join(ratings_df2,movies_df2.MovieId == ratings_df2.MovieId,"inner").\
                    select("Title","UserId").\
                    groupBy("Title").agg(count("UserId").alias("Views")).\
                    orderBy(desc("Views")).limit(10)
# -------------------------------------------------------------------------------------------------------------------------
# Solution 2:
    movies_ratings2 = movies_df2.join(ratings_df2,movies_df2.MovieId == ratings_df2.MovieId,"inner").\
                    select("Title","UserId","rating").\
                    groupBy("Title").agg(count("UserId").alias("Views"),avg("rating").alias("AvgRating")).\
                    filter(col("Views")>=40).orderBy(desc("AvgRating")).limit(20)
    movies_df2.createOrReplaceTempView("movie_table")
    ratings_df2.createOrReplaceTempView("ratings_table")              
# ---------------------------------------------------------------------------------------------------------------------------------
# Solution 1 ANSI SQL
query1 = """
    select Title,count("UserId") as Views from
    (select m.Title,r.UserId from movie_table m join ratings_table r on m.MovieId = r.MovieId)
    group by Title order by Views desc limit 10
    """
sql(query1)
query2 = """
        select m.Title,count(r.UserId) as views from movie_table m join ratings_table r on m.MovieId = r.MovieId group by Title order by views desc limit 10 
"""
sql(query2)
-----------------------------------------------------------------------------------------------------------------
# spark sql wordcount code
df1 = spark.read.format("text").load("file")
df1.select(explode(split("value"," "))).groupBy("col").agg(count("*").alias("count")).orderBy(desc("count")).show(10)


