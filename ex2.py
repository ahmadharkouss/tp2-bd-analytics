from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max, avg, col, year

# Create SparkSession
spark = (((SparkSession.builder
         .master("local[*]"))
         .appName("taxi-ex1")
         .config("spark.driver.memory", "6g"))
         .getOrCreate())

# Set log level to ERROR
spark.sparkContext.setLogLevel("ERROR")

input_path = "/home/enno/Documents/EPITA/Big Data Analytics/TP/TP5/data/yellow taxi/all-2022.parquet"
data = spark.read.parquet("file://" + input_path)

# Q1 : Calculer le nombre total de trajets dans le dataset
total_instances = data.count()
print("Total instances in dataset: " + str(total_instances))

# Q2 : Calculer le plus petit montant et le plus grand montant enregistré
min_amount = data.agg(min("total_amount")).collect()[0][0]
max_amount = data.agg(max("total_amount")).collect()[0][0]
print(f"Smallest amount: {min_amount}")
print(f"Biggest amount: {max_amount}")

# Q3 : Garder uniquement les lignes avec des montants positifs ainsi que des montants inférieurs à 1000$
print("Filtering on amount paid...")
data = data.filter((data.total_amount >= 0) & (data.total_amount <= 1000))
print(f"Number of instances removed: {total_instances - data.count()}")

# Q4 : Calculer des statistiques sur la distance des trajets
min_trip_distance = data.agg(min("trip_distance")).collect()[0][0]
max_trip_distance = data.agg(max("trip_distance")).collect()[0][0]
avg_trip_distance = data.agg(avg("trip_distance")).collect()[0][0]
print("Minimum trip_distance:", min_trip_distance)
print("Maximum trip_distance:", max_trip_distance)
print("Average trip_distance:", avg_trip_distance)

# Q5 : Garder que les trajets avec une distance strictement positive et ne dépassant pas les 100 miles
total_instances = data.count()
print("Filtering on trip distance...")
data = data.filter((data.trip_distance > 0) & (data.trip_distance <= 100))
print(f"Number of instances removed: {total_instances - data.count()}")

# Q6 : Garder que les trajets avec un nombre de passagers cohérent (on se propose d’utiliser l’intervalle [1-10])
total_instances = data.count()
print("Filtering on passenger count...")
data = data.filter((data.passenger_count >= 1) & (data.passenger_count <= 10))
print(f"Number of instances removed: {total_instances - data.count()}")

# Q7 : Quels sont les nombres de passagers les plus fréquents ?
passenger_count_distribution = data.groupBy("passenger_count").count().orderBy("passenger_count")
most_frequent_passenger_count = data.groupBy("passenger_count").count().orderBy(col("count").desc()).first()[0]
print("Distribution of passenger_count:")
passenger_count_distribution.show()
print("Most frequent number of passenger_count:", most_frequent_passenger_count)

# Q8 : Supprimer les lignes qui n'ont pas une date de début et de fin de trajet en 2022
total_instances = data.count()
print("Filtering on pickup and dropoff dates...")
data = data.filter((year(col("tpep_pickup_datetime")) == 2022) & (year(col("tpep_dropoff_datetime")) == 2022))
print(f"Number of instances removed: {total_instances - data.count()}")

print("Saving cleaned data into one single file...")
output_path = "/home/enno/Documents/EPITA/Big Data Analytics/TP/TP5/data/yellow taxi/all-2022-cleaned.parquet"
data.write.parquet(output_path)
print("Done!")
