from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("ex5-q1") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# Set log level
spark.sparkContext.setLogLevel("ERROR")

# Load the RDD
rdd = spark.sparkContext.textFile("file:///home/ahmad/bd-analytics/data/725053-94728-2022")

# Filter and map to extract day and temperature
temperature_rdd = rdd.filter(lambda line: len(line) >= 92) \
    .map(lambda line: (line[15:23], line[87], float(line[88:92]) / 10))   \
    .filter(lambda x: x[2] != 999.9) # ignore missing values usually reported as 9999 in ncdc noaa dataset

#print(temperature_rdd.collect())

# Adjust the temperature based on the sign
adjusted_temperature_rdd = temperature_rdd.map(lambda x: (x[0], x[2] * (-1 if x[1] == '-' else 1)))

# Reduce by key to calculate the sum and count of temperatures for each day
sum_count_rdd = adjusted_temperature_rdd.mapValues(lambda x: (x, 1)) \
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

# Map again to calculate the average temperature for each day
average_temperature_rdd = sum_count_rdd.mapValues(lambda x: x[0] / x[1])

#print(average_temperature_rdd.collect())

# Convert RDD to DataFrame
temperature_df = average_temperature_rdd.toDF(["Day", "AverageTemperature"])

# Show the DataFrame
temperature_df.show()

