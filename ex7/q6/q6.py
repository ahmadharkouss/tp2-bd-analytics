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

ratings_df = spark.read.csv("file:///home/ahmad/bd-analytics/data/ratings.dat", sep="::", inferSchema=True).toDF("userId", "movieId", "rating", "timestamp")


# Q6 - Nombre de films pour chaque appréciation (rating 1, 2, 3, 4 et 5)
ratings_count = ratings_df.groupBy("rating").count().orderBy("rating")
print("Nombre de films pour chaque appréciation (rating 0.5, 1, 2, 3, 4 et 5) :")
ratings_count.show()
