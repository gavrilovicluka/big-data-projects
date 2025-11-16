import time
from utils import print_separator, get_args, show_delays, calculate_statistics_by_group, analyze_weather_impact_on_delays

from pyspark.sql import SparkSession
from pyspark.sql.functions import when, sum, col, avg, desc, stddev, min, max, mean, round

# DATA_PATH = "./data/full_data_flightdelay.csv"
DATA_PATH = "hdfs://namenode:9000/dir/full_data_flightdelay.csv"
APP_NAME = "FlightDelays"
# SPARK_MASTER = "spark://spark-master:7077"
SPARK_MASTER = "local[*]"

def initialize(args):
    spark_session = SparkSession.builder \
        .appName(APP_NAME) \
        .getOrCreate()
    
    print("Current Spark master:", spark_session.sparkContext.master)
    
    spark_session.sparkContext.setLogLevel("ERROR")  # Set the log level to ERROR to reduce the amount of output

    data_frame = spark_session.read.csv(args.input, header=True, inferSchema=True)

    return spark_session, data_frame


def analyze(df, args):
    month_str = f"month {args.month}" if args.month else ""
    carrier_str = f"carrier {args.carrier}" if args.carrier else None

    show_delays(df, group_by="MONTH", title="Flights with delay ≥15 min per month")
    
    show_delays(df, group_by="CARRIER_NAME", title="Flights with delay ≥15 min per airline")

    show_delays(df, group_by="DEPARTING_AIRPORT", top_n=10, title="Top 10 airports with highest number of delays ≥15 min")

    if args.month:
        show_delays(df, group_by="MONTH", title=f"Flights with delay ≥15 min in {month_str}", month=args.month)
        show_delays(df, group_by="CARRIER_NAME", title=f"Flights with delay ≥15 min per airline in {month_str}", month=args.month)

    if args.carrier:
        show_delays(df, group_by="MONTH", title=f"Delayed flights for {carrier_str} per month", carrier_name=args.carrier)
    
    if args.group_by_col and args.stats_col:
        stats_by_group = calculate_statistics_by_group(df, args.group_by_col, args.stats_col)
        if stats_by_group:
            print_separator(f"{args.stats_col} statistics by {args.group_by_col}")
            stats_by_group.show(10, truncate=False)
    # stats_plane_age_by_carrier = calculate_statistics_by_group(df, "CARRIER_NAME", "PLANE_AGE")
    # if stats_plane_age_by_carrier:
    #     print_separator("Airplane age statistics by airline")
    #     stats_plane_age_by_carrier.show(10, truncate=False)

    # stats_seats_by_airport = calculate_statistics_by_group(df, "DEPARTING_AIRPORT", "NUMBER_OF_SEATS")
    # if stats_seats_by_airport:
    #     print_separator("Seat number statistics by departure airport")
    #     stats_seats_by_airport.show(10, truncate=False)

    print_separator("Airport average monthly pass")
    df.select(
        min("AVG_MONTHLY_PASS_AIRPORT"), 
        max("AVG_MONTHLY_PASS_AIRPORT"), 
        mean("AVG_MONTHLY_PASS_AIRPORT"), 
        stddev("AVG_MONTHLY_PASS_AIRPORT")
    ).show()

    # Effect of temperature (TMAX) and precipitation (PRCP) on delays ≥15 min
    print_separator("Effect of weather conditions on delays (month, avg TMAX, avg PRCP)")
    df.filter(col("DEP_DEL15") == 1) \
        .groupBy("MONTH") \
        .agg(
            round(avg("TMAX"), 2).alias("AvgTMAX"),
            round(avg("PRCP"), 2).alias("AvgPRCP")
        ) \
        .orderBy("MONTH") \
        .show()

    # By measurement stations: average number of airline flights per airport
    print_separator("Average number of airline flights per airport")
    df.groupBy("DEPARTING_AIRPORT", "CARRIER_NAME") \
        .agg(
            round(avg("AIRLINE_FLIGHTS_MONTH"), 2).alias("AvgMonthlyFlightsPerCarrier")
        ) \
        .orderBy(desc("AvgMonthlyFlightsPerCarrier")) \
        .show(10, False)

    print_separator("Weather impact on delays")
    analyze_weather_impact_on_delays(df)


if __name__ == "__main__":
    args = get_args()

    # Initialize Spark session and DataFrame
    spark, df = initialize(args)

    print("Count of dataset:" + str(df.count()))
    print("\nNumber of columns:" + str(len(df.columns)))

    print_separator("Dataset")
    df.show(10)

    print_separator("Schema")
    df.printSchema()

    print_separator("Missing values")
    missing_values_df = df.select([
        sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) for c in df.columns
    ])
    missing_values_df.show()

    print("\n*****************************************************************************")
    print_separator("Analysis")
    print("*****************************************************************************\n")

    start = time.time()
    analyze(df, args)
    end = time.time()

    print("\n*****************************************************************************\n")
    print("Duration:", end - start, "s")
    print("\n*****************************************************************************\n")

    spark.stop()
