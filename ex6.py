from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

# Create SparkSession
spark = (((SparkSession.builder
         .master("local[*]"))
         .appName("taxi-ex1")
         .config("spark.driver.memory", "6g"))
         .getOrCreate())

# Set log level to ERROR
spark.sparkContext.setLogLevel("ERROR")

input_path = "/home/enno/Documents/EPITA/Big Data Analytics/TP/TP5/data/yellow taxi/all-2022-cleaned.parquet"
data = spark.read.parquet("file://" + input_path)

# Calculate trip duration in minutes
trip_duration_minutes = (unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) / 60

# Select features (distance and duration) and target variable (total_amount)
data = data.select("trip_distance", trip_duration_minutes.alias("trip_duration"), "total_amount")

# Prepare features vector
assembler = VectorAssembler(inputCols=["trip_distance", "trip_duration"], outputCol="features")
data = assembler.transform(data)

# Split the data into training and testing sets (80% train, 20% test)
train_data, test_data = data.randomSplit([0.8, 0.2], seed=42)

# Train the linear regression model
lr = LinearRegression(featuresCol="features", labelCol="total_amount")
model = lr.fit(train_data)

# Evaluate the model on testing data
evaluator = RegressionEvaluator(labelCol="total_amount", predictionCol="prediction", metricName="rmse")
predictions = model.transform(test_data)
rmse = evaluator.evaluate(predictions)
print("Root Mean Squared Error (RMSE) on test data:", rmse)

# Make predictions on new data
new_data = spark.createDataFrame([(4.0, 20.0)], ["trip_distance", "trip_duration"])
new_data = assembler.transform(new_data)
predicted_amount = model.transform(new_data).select("prediction").collect()[0][0]
print("Predicted total amount for new trip:", predicted_amount)
