from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, count, date_format , regexp_extract

# Create SparkSession
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("ex5-q1") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# Set log level
spark.sparkContext.setLogLevel("ERROR")


movies_df = spark.read.csv("file:///home/ahmad/bd-analytics/data/movies.dat", sep="::", inferSchema=True).toDF("movieId", "title", "genres")
ratings_df = spark.read.csv("file:///home/ahmad/bd-analytics/data/ratings.dat", sep="::", inferSchema=True).toDF("userId", "movieId", "rating", "timestamp")


# Q2 - Nombre de films dans le jeu de données
num_movies = movies_df.count()
print("Nombre de films dans le jeu de données :", num_movies)