import argparse
from pyspark.sql.functions import col, count, mean, stddev, round, min, max


def print_separator(title):
    print("\n")
    print("--------------------------------- " + title + " ---------------------------------")
    print("\n")


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Path to CSV file (Local or HDFS)")
    parser.add_argument("--month", type=int, default=None)
    parser.add_argument("--carrier", type=str, default=None, help="Airline name")
    parser.add_argument("--stats_col", type=str, default="PLANE_AGE", help="Column to calculate statistics on")
    parser.add_argument("--group_by_col", type=str, default="CARRIER_NAME", help="Column to group by for statistics")
    return parser.parse_args()


def show_delays(df, title, group_by=None, top_n=None, month=None, carrier_name=None):
    """
    Shows number of delayed flights (DEP_DEL15 == 1) grouped by a column,
    optionally filtered by month.

    Parameters:
    - df: Spark DataFrame
    - title: str, title to print before output
    - group_by: column name to group by (str)
    - top_n: int, number of top results to show (optional)
    - month: int, optional month to filter by (1-12)
    - carrier_name: str, optional name of airline
    """
    print_separator(title)

    filtered_df = df.filter(col("DEP_DEL15") == 1)

    if month is not None:
        filtered_df = filtered_df.filter(col("MONTH") == month)
    if carrier_name is not None:
        filtered_df = filtered_df.filter(col("CARRIER_NAME") == carrier_name)

    if group_by:
        result = filtered_df.groupBy(group_by).agg(count("*").alias("DelayedFlightsNum"))
        if top_n:
            result = result.sort(col("DelayedFlightsNum").desc()).limit(top_n)
        else:
            result = result.orderBy(group_by)
    else:
        result = filtered_df

    result.show(truncate=False)


def calculate_statistics_by_group(df, group_by_col, statistic_col):
    """
    Calculates statistical parameters (min, max, avg, stddev)
    for a given attribute, grouped by another attribute.
    """
    if group_by_col not in df.columns or statistic_col not in df.columns:
        print(f"Error: Columns '{group_by_col}' or '{statistic_col}' does not exist.")
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
    Analyzes the impact of weather conditions (rainfall, snow, temperature) on flight delays.
    Shows weather statistics for delayed and non-delayed flights.
    PRCP: Inches of precipitation for day
    SNOW: Inches of snowfall for day
	TMAX: Max temperature for day
	AWND: Max wind speed for day
    """
    delayed_flights = df.filter(col("DEP_DEL15") == 1)
    on_time_flights = df.filter(col("DEP_DEL15") == 0)

    print("--- Delayed flights ---")
    delayed_flights.select(
        round(mean("PRCP"), 2).alias("avg_prcp_delayed"),
        round(mean("SNOW"), 2).alias("avg_snow_delayed"),
        round(mean("TMAX"), 2).alias("avg_temp_delayed"),
        round(mean("AWND"), 2).alias("avg_wind_delayed")
    ).show()

    print("\n--- On-time flights ---")
    on_time_flights.select(
        round(mean("PRCP"), 2).alias("avg_prcp_on_time"),
        round(mean("SNOW"), 2).alias("avg_snow_on_time"),
        round(mean("TMAX"), 2).alias("avg_temp_on_time"),
        round(mean("AWND"), 2).alias("avg_wind_on_time")
    ).show()
