from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, hour, concat, lit, avg, count, expr, unix_timestamp
import altair as alt
from altair_saver import save

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


# Q1 : Calculer le nombre de trajets effectués pour chaque jour de l’année
# Q2 : Créer un graphique qui montre cette évolution
def questions_1_2():
    trips_per_day = data.groupBy(date_format(col("tpep_pickup_datetime"), "yyyy-MM-dd").alias("pickup_date")) \
                    .count() \
                    .orderBy("pickup_date")

    # Convertir le DataFrame PySpark en DataFrame Pandas pour Altair
    trips_per_day_df = trips_per_day.toPandas()

    chart = alt.Chart(trips_per_day_df).mark_line().encode(
        x='pickup_date',
        y='count',
        tooltip=['pickup_date', 'count']
    ).properties(
        width=800,
        height=400,
        title='Nombre de trajets effectués pour chaque jour de l’année 2022'
    )

    save(chart, "./DataViz/trips_per_day.html")

# Q3 : Calculer le nombre de trajets pour chaque jour
# Q4 : Créer un histogramme qui montre la distribution
def questions_3_4():
    data_with_day_of_week = data.withColumn("day_of_week", date_format(col("tpep_pickup_datetime"), "EEEE"))

    trips_per_day_of_week = data_with_day_of_week.groupBy("day_of_week").count()

    chart = alt.Chart(trips_per_day_of_week.toPandas()).mark_bar().encode(
        x='day_of_week',
        y='count',
        tooltip=['day_of_week', 'count']
    ).properties(
        width=600,
        height=400,
        title='Distribution des trajets par jour de la semaine'
    )

    save(chart, "./DataViz/trips_per_day_of_week.html")


# Q5 : Calculer le nombre de trajets et la distance moyenne pour chaque intervalle
# Q6 : Créer les visualisations qui montrent ces distributions
def questions_5_6():
    data_with_hour = data.withColumn("hour", hour(col("tpep_pickup_datetime")))

    trips_per_hour = data_with_hour.groupBy("hour").count()
    avg_distance_per_hour = data_with_hour.groupBy("hour").agg(avg("trip_distance").alias("avg_trip_distance"))

    chart1 = alt.Chart(trips_per_hour.toPandas()).mark_bar().encode(
        x='hour',
        y='count',
        tooltip=['hour', 'count']
    ).properties(
        width=600,
        height=400,
        title='Distribution des trajets par heure de la journée'
    )

    chart2 = alt.Chart(avg_distance_per_hour.toPandas()).mark_bar().encode(
        x='hour',
        y='avg_trip_distance',
        tooltip=['hour', 'avg_trip_distance']
    ).properties(
        width=600,
        height=400,
        title='Distance moyenne des trajets par heure de la journée'
    )

    save(chart1, "./DataViz/trips_per_hour.html")
    save(chart2, "./DataViz/avg_distance_per_hour.html")


# Q7 : Quelles méthodes de paiement sont les plus populaires ? Afficher le pourcentage de chacune des catégories de paiement
def question_7():
    # Calculate payment distribution
    payment_distribution = data.groupBy("payment_type").agg(count("*").alias("count"))

    # Calculate total number of payments
    total_payments = data.count()

    # Calculate percentage of each payment type
    payment_distribution = payment_distribution.withColumn("percentage", (col("count") / total_payments) * 100)

    # Define color scheme for payment types
    color_scheme = alt.Scale(domain=['1', '2', '3', '4', '5', '6'], range=['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b'])

    # Create chart
    chart = alt.Chart(payment_distribution.toPandas()).mark_arc().encode(
        theta="percentage",
        color=alt.Color("payment_type", scale=color_scheme),
        tooltip=['payment_type', 'percentage']
    ).properties(
        width=600,
        height=400,
        title='Distribution des méthodes de paiement'
    )

    # Save chart as HTML
    save(chart, "./DataViz/payment_distribution.html")


# Q8 : Quelle est la durée moyenne d’un trajet pour chaque mois ?
def question_8():
    # Calculate trip duration in minutes
    trip_duration_minutes = (unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(
        col("tpep_pickup_datetime"))) / 60

    # Calculate average trip duration for each month
    avg_trip_duration_per_month = data.withColumn("trip_duration_minutes", trip_duration_minutes) \
        .groupBy(date_format(col("tpep_pickup_datetime"), "yyyy-MM").alias("pickup_month")) \
        .agg(avg("trip_duration_minutes").alias("avg_trip_duration_minutes"))

    # Create chart
    chart = alt.Chart(avg_trip_duration_per_month.toPandas()).mark_bar().encode(
        x='pickup_month',
        y='avg_trip_duration_minutes',
        tooltip=['pickup_month', 'avg_trip_duration_minutes']
    ).properties(
        width=600,
        height=400,
        title='Durée moyenne d’un trajet pour chaque mois (en minutes)'
    )

    # Save chart as HTML
    save(chart, "./DataViz/avg_trip_duration_per_month.html")


# Q9 : Est ce que les clients donnent plus de pourboires pendant les weekends comparé aux autres jours de la semaine ?
def question_9():
    # Calculate tip percentage
    tip_percentage = (col("tip_amount") / col("total_amount")) * 100

    # Add tip percentage column to DataFrame
    data_with_tip_percentage = data.withColumn("tip_percentage", tip_percentage)

    # Extract day of the week from pickup datetime
    data_with_day_of_week = data_with_tip_percentage.withColumn("day_of_week", date_format(col("tpep_pickup_datetime"), "EEEE"))

    # Calculate average tip percentage for each day of the week
    avg_tip_percentage_per_day_of_week = data_with_day_of_week.groupBy("day_of_week") \
        .agg(avg("tip_percentage").alias("avg_tip_percentage"))

    # Create chart
    chart = alt.Chart(avg_tip_percentage_per_day_of_week.toPandas()).mark_bar().encode(
        x='day_of_week',
        y='avg_tip_percentage',
        tooltip=['day_of_week', 'avg_tip_percentage']
    ).properties(
        width=600,
        height=400,
        title='Pourcentage moyen de pourboires pour chaque jour de la semaine'
    )

    # Save chart as HTML
    save(chart, "./DataViz/avg_tip_percentage_per_day_of_week.html")


def main():
    #questions_1_2()
    #questions_3_4()
    #questions_5_6()
    #question_7()
    #question_8()
    question_9()


if __name__ == "__main__":
    main()
