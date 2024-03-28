from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, count, date_format , regexp_extract, explode, split

# Create SparkSession
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("ex5-q1") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# Set log level
spark.sparkContext.setLogLevel("ERROR")


movies_df = spark.read.csv("file:///home/ahmad/bd-analytics/data/movies.dat", sep="::", inferSchema=True).toDF("movieId", "title", "genres")
#
genres_list_df = movies_df.select(explode(split(col("genres"), "\\|")).alias("genre"))
all_genres = genres_list_df.select("genre").distinct().rdd.map(lambda row: row[0]).collect()



# Q5 - Liste de films par genres
genre_movies = {}
for genre in all_genres:
    genre_movies[genre] = movies_df.filter(col("genres").contains(genre)).select("title").rdd.map(lambda row: row[0]).collect()

#create a txt file for each genre
for genre, movies in genre_movies.items():
    with open(f"/home/ahmad/bd-analytics/hadoop-spark/tp2-bd-analytics/ex7/q5/genres/{genre}.txt", "w") as file:
        for movie in movies:
            file.write(f"{movie}\n")