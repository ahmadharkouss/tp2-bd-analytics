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

# Q3 - Ajouter une colonne pour l'année de sortie de chaque film
#regex expression extract the end of the title string that contains the year with the format (yyyy)
movies_df = movies_df.withColumn("release_year", regexp_extract(col("title"), "\((\d{4})\)$", 1))

movies_df.printSchema()
movies_df.show()
