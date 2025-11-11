import sys

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, sum, col, avg, desc, count, stddev, min, max, mean, round

DATA_PATH = "./data/full_data_flightdelay.csv"
# DATA_PATH = "hdfs://namenode:9000/dir/full_data_flightdelay.csv"
APP_NAME = "FlightDelays"
SPARK_MASTER = "spark://spark-master:7077"


def initialize():
    # Define Spark Configuration
    conf = SparkConf()

    # conf.setMaster(SPARK_MASTER)
    # conf.setMaster("local[*]")

    spark_session = SparkSession.builder.master("local[*]").appName(APP_NAME).getOrCreate()
    # Set the log level to ERROR to reduce the amount of output
    spark_session.sparkContext.setLogLevel("ERROR")

    data_frame = spark_session.read.csv(DATA_PATH, header=True, inferSchema=True)

    return spark_session, data_frame


def analyze(df):
    # 1. Broj letova sa `DEP_DEL15 == 1` po mesecu
    print("Letovi sa kašnjenjem ≥15 min po mesecu:")
    df.filter(col("DEP_DEL15") == 1) \
        .groupBy("MONTH") \
        .agg(count("*").alias("DelayedFlights")) \
        .orderBy("MONTH") \
        .show()

    # 2. Statistika distance_group kod kašnjenja po aviokompaniji
    print("Statistika DISTANCE_GROUP za kašnjenje ≥15′ po aviokompaniji:")
    df.filter(col("DEP_DEL15") == 1) \
        .groupBy("CARRIER_NAME") \
        .agg(
        count("*").alias("Count"),
        avg("DISTANCE_GROUP").alias("AvgDistanceGroup"),
        min("DISTANCE_GROUP").alias("MinDistanceGroup"),
        max("DISTANCE_GROUP").alias("MaxDistanceGroup"),
        stddev("DISTANCE_GROUP").alias("StddevDistanceGroup")
    ) \
        .orderBy(desc("AvgDistanceGroup")) \
        .show(10, False)

    # 3. Uticaj temperature (TMAX) i padavina (PRCP) na kašnjenja ≥15′
    print("Uticaj vremenskih uslova na kašnjenja (mesec, avg TMAX, avg PRCP):")
    df.filter(col("DEP_DEL15") == 1) \
        .groupBy("MONTH") \
        .agg(
        avg("TMAX").alias("AvgTMAX"),
        avg("PRCP").alias("AvgPRCP")
    ) \
        .orderBy("MONTH") \
        .show()

    # 4. Po mernim stanicama: prosečan broj letova aviokompanije po aerodromu
    print("Prosečan broj letova aviokompanije po aerodromu:")
    df.groupBy("DEPARTING_AIRPORT", "CARRIER_NAME") \
        .agg(avg("AIRLINE_FLIGHTS_MONTH").alias("AvgMonthlyFlightsPerCarrier")) \
        .orderBy(desc("AvgMonthlyFlightsPerCarrier")) \
        .show(10, False)


def count_delayed_flights(df, month=None, carrier_name=None):
    """
    Određuje broj letova sa kašnjenjem (>15min, DEP_DEL15=1)
    za zadati mesec i avio-kompaniju.
    """
    filtered_df = df.filter(col("DEP_DEL15") == 1)

    if month:
        filtered_df = filtered_df.filter(col("MONTH") == month)

    if carrier_name:
        filtered_df = filtered_df.filter(col("CARRIER_NAME") == carrier_name)

    return filtered_df.count()


def analyze_delayed_flights_by_airport(df):
    """
    Analizira broj kašnjenja po aerodromu i prikazuje 10 aerodroma
    sa najvećim brojem kašnjenja.
    """
    delayed_by_airport = df.filter(col("DEP_DEL15") == 1) \
        .groupBy("DEPARTING_AIRPORT") \
        .agg(count("DEP_DEL15").alias("num_delays")) \
        .sort(col("num_delays").desc())

    print("\n10 aerodroma sa najvećim brojem kašnjenja:")
    delayed_by_airport.show(10, truncate=False)


def calculate_statistics_by_group(df, group_by_col, statistic_col):
    """
    Izračunava statističke parametre (min, max, avg, stddev)
    za dati atribut, grupisano po drugom atributu.
    """
    if group_by_col not in df.columns or statistic_col not in df.columns:
        print(f"Greška: Kolona '{group_by_col}' ili '{statistic_col}' ne postoji.")
        return None

    stats_df = df.groupBy(group_by_col).agg(
        round(mean(statistic_col), 2).alias(f"average_{statistic_col}"),
        round(min(statistic_col), 2).alias(f"min_{statistic_col}"),
        round(max(statistic_col), 2).alias(f"max_{statistic_col}"),
        round(stddev(statistic_col), 2).alias(f"stddev_{statistic_col}"),
        count("*").alias("total_records")
    )

    return stats_df.sort(col("total_records").desc())


def analyze_weather_impact_on_delays(df):
    """
    Analizira uticaj vremenskih uslova (padavina, snega, temperature) na kašnjenja letova.
    Prikazuje statistike o vremenskim uslovima za letove koji su kasnili i one koji nisu.
    """
    # Kreiranje uslova za kašnjenje (DEP_DEL15 == 1) i bez kašnjenja (DEP_DEL15 == 0)
    delayed_flights = df.filter(col("DEP_DEL15") == 1)
    on_time_flights = df.filter(col("DEP_DEL15") == 0)

    print("\n--- Analiza vremenskih uslova za letove koji su kasnili ---")
    delayed_flights.select(
        round(mean("PRCP"), 2).alias("avg_prcp_delayed"),
        round(mean("SNOW"), 2).alias("avg_snow_delayed"),
        round(mean("TMAX"), 2).alias("avg_tmax_delayed"),
        round(mean("AWND"), 2).alias("avg_awnd_delayed")
    ).show()

    print("\n--- Analiza vremenskih uslova za letove koji nisu kasnili ---")
    on_time_flights.select(
        round(mean("PRCP"), 2).alias("avg_prcp_on_time"),
        round(mean("SNOW"), 2).alias("avg_snow_on_time"),
        round(mean("TMAX"), 2).alias("avg_tmax_on_time"),
        round(mean("AWND"), 2).alias("avg_awnd_on_time")
    ).show()


def analyze_flights_per_passenger(df):
    """
    Izračunava odnos broja letova po putniku za svaku avio-kompaniju
    i prikazuje statistike.
    """
    # Izbegavamo deljenje sa nulom i kolone koje nisu numeričke
    df = df.filter((col("AVG_MONTHLY_PASS_AIRLINE") > 0) & (col("AIRLINE_FLIGHTS_MONTH") > 0))

    # Kreiranje nove kolone koja predstavlja broj letova po putniku
    flights_per_passenger_df = df.withColumn(
        "FLIGHTS_PER_PASSENGER",
        col("AIRLINE_FLIGHTS_MONTH") / col("AVG_MONTHLY_PASS_AIRLINE")
    )

    print("\n--- Analiza odnosa letova po putniku za avio-kompanije ---")
    flights_per_passenger_df.groupBy("CARRIER_NAME").agg(
        round(mean("FLIGHTS_PER_PASSENGER"), 4).alias("avg_flights_per_passenger"),
        round(stddev("FLIGHTS_PER_PASSENGER"), 4).alias("stddev_flights_per_passenger"),
        count("*").alias("total_records")
    ).sort(col("avg_flights_per_passenger").desc()).show(10, truncate=False)


if __name__ == "__main__":
    # Argumenti aplikacije:
    # 1. prag kašnjenja (npr. 30 minuta)
    # 2. šifra avio-kompanije (opciono, npr. 'UA')
    # 3. početni datum (opciono, '2019-01-01')
    # 4. krajnji datum (opciono, '2019-01-31')
    # 5. kolona za grupisanje za statistiku (npr. 'ORIGIN_AIRPORT')
    # 6. kolona za statistiku (npr. 'DEPARTURE_DELAY')

    # if len(sys.argv) < 3:
    #     print(
    #         "Upotreba: spark-submit --master spark://spark-master:7077 your_script.py <threshold> <group_by_col> <stats_col> [<airline_code> <start_date> <end_date>]")
    #     sys.exit(-1)

    # if len(sys.argv) != 2:
    #     print("Usage: wordcount <file>", file=sys.stderr)
    #     sys.exit(-1)

    # Parsiranje argumenata
    # delay_threshold = float(sys.argv[1])
    # group_by_column = sys.argv[2]
    # statistic_column = sys.argv[3]
    # airline_arg = sys.argv[4] if len(sys.argv) > 4 else None
    # start_date_arg = sys.argv[5] if len(sys.argv) > 5 else None
    # end_date_arg = sys.argv[6] if len(sys.argv) > 6 else None

    # Initialize Spark session and DataFrame
    spark, df = initialize()

    print("Count of dataset:" + str(df.count()))
    print("\nNumber of columns:" + str(len(df.columns)))

    print("\nDataset: \n")
    df.show(10)

    print("Schema:")
    df.printSchema()

    missing_values_df = df.select([
        sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) for c in df.columns
    ])
    missing_values_df.show()

    analyze(df)

    print("\n**********************************************************************\n")

    # Funkcionalnost 1: Odrediti broj i karakteristike
    print("\n--- Analiza broja kašnjenja (Funkcionalnost 1) ---")

    # Primer 1: Broj letova sa kašnjenjem u martu (MONTH = 3)
    num_delayed_march = count_delayed_flights(df, month=3)
    print(f"Broj letova sa kašnjenjem u martu: {num_delayed_march}")

    # Primer 2: Broj letova sa kašnjenjem za 'United Air Lines Inc.'
    num_delayed_united = count_delayed_flights(df, carrier_name="United Air Lines Inc.")
    print(f"Broj letova sa kašnjenjem za 'United Air Lines Inc.': {num_delayed_united}")

    # Primer 3: Karakteristike kašnjenja - analiza po aerodromima
    analyze_delayed_flights_by_airport(df)

    # Funkcionalnost 2: Statistički parametri
    print("\n--- Statistička analiza (Funkcionalnost 2) ---")

    # Primer 1: Statistika starosti aviona grupisana po avio-kompaniji
    stats_plane_age_by_carrier = calculate_statistics_by_group(df, "CARRIER_NAME", "PLANE_AGE")
    if stats_plane_age_by_carrier:
        print("Statistika starosti aviona po avio-kompaniji:")
        stats_plane_age_by_carrier.show(10, truncate=False)

    # Primer 2: Statistika broja sedišta grupisana po aerodromu polaska
    stats_seats_by_airport = calculate_statistics_by_group(df, "DEPARTING_AIRPORT", "NUMBER_OF_SEATS")
    if stats_seats_by_airport:
        print("\nStatistika broja sedišta po aerodromu polaska:")
        stats_seats_by_airport.show(10, truncate=False)

    # Dodatna analiza 1: Uticaj vremenskih prilika na kašnjenja
    analyze_weather_impact_on_delays(df)

    # Dodatna analiza 2: Odnos letova po putniku za avio-kompanije
    analyze_flights_per_passenger(df)

    spark.stop()
