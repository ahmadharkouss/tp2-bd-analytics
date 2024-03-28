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


genres_list_df = movies_df.select(explode(split(col("genres"), "\\|")).alias("genre"))

# Obtenir la liste unique de tous les genres disponibles
all_genres = genres_list_df.select("genre").distinct().rdd.map(lambda row: row[0]).collect()

# Afficher la liste de tous les genres de films disponibles
print("Liste de tous les genres de films disponibles :\n")
for genre in all_genres:
    print("-",genre, end="\n")

