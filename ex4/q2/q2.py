from pyspark.sql import SparkSession
from pyspark.sql.functions import count, desc

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("taxi-ex1") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


# Read data1 from parquet file
data1 = spark.read.option("header", "true").option("inferschema", "true").parquet("file:///home/ahmad/bd-analytics/data/yellow_tripdata_2022-01.parquet")

# Read data2 from a CSV file
data2 = spark.read.option("header", "true").option("inferschema", "true").csv("file:///home/ahmad/bd-analytics/data/taxi_zone_lookup.csv")

# Join data1 with data2 for PULocationID
data_pu = data1.join(data2.withColumnRenamed("LocationID", "PULocationID")
                    .withColumnRenamed("Borough", "PUBorough")
                    .withColumnRenamed("Zone", "PUZone")
                    .withColumnRenamed("service_zone", "PUservice_zone"),
                    "PULocationID")


# Join data1 with data2 for DOLocationID
data_final = data_pu.join(data2.withColumnRenamed("LocationID", "DOLocationID")
                    .withColumnRenamed("Borough", "DOBorough")
                    .withColumnRenamed("Zone", "DOZone")
                    .withColumnRenamed("service_zone", "DOservice_zone"),
                    "DOLocationID")

#Calculate the number of trips for each destination zone
destination_counts = data_final.groupBy("DOZone").agg(count("*").alias("trip_count"))

#Select the top 4 destination zones based on trip count
top_destinations = destination_counts.orderBy(desc("trip_count")).limit(4)

print("Top 4 destination zones based on trip count:")
top_destinations.show()


#For each of the top 4 destination zones, find the starting zones of the corresponding trips
counter=1;
final_result = []
for destination in top_destinations.collect():
    destination_zone = destination["DOZone"]
    starting_zones = data_final.filter(data_final["DOZone"] == destination_zone).groupBy("PUZone").agg(count("*").alias("trip_count_from_starting_zone"))
    top_starting_zones = starting_zones.orderBy(desc("trip_count_from_starting_zone")).limit(5)
    print(f"Top5 Starting zones for top {counter} destination zone: ", destination_zone)
    top_starting_zones.show()
    total_trips = top_starting_zones.selectExpr("sum(trip_count_from_starting_zone) as total_trips").collect()[0]["total_trips"]
    final_result.append((destination_zone, total_trips))
    counter+=1

for result in final_result:
    print(f"Total trips from top 5 starting zones to destination zone {result[0]}: {result[1]}")
