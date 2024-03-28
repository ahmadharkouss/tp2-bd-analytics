from pyspark.sql import SparkSession

# Create SparkSession
spark = (((SparkSession.builder
         .master("local[*]"))
         .appName("taxi-ex1")
         .config("spark.driver.memory", "6g"))
         .getOrCreate())


# Set log level to ERROR
spark.sparkContext.setLogLevel("ERROR")

# Initialize an empty DataFrame to hold the combined data
data = None

# Loop through each month from January to December
for month in range(1, 13):
    # Construct the path to the Parquet file for the current month
    file_path = f"file:///home/enno/Documents/EPITA/Big Data Analytics/TP/TP5/data/yellow taxi/yellow_tripdata_2022-{month:02d}.parquet"

    # Read the Parquet file into a DataFrame
    month_data = spark.read.parquet(file_path)

    # Union the current month's data with the combined data
    if data is None:
        data = month_data
    else:
        data = data.union(month_data)

# Show the DataFrame
data.show()
data.printSchema()

#print("Saving data from all months into one single file...")
## Save the combined DataFrame back to your computer in Parquet format
#output_path = "/home/enno/Documents/EPITA/Big Data Analytics/TP/TP5/data/yellow taxi/all-2022.parquet"
#data.write.parquet(output_path)
#print("Done!")
#
#print("Test loading the saved file")
#saved_data = spark.read.parquet("file://" + output_path)
#saved_data.show()
