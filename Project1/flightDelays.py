from pyspark import SparkConf
from pyspark.sql import SparkSession

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


if __name__ == "__main__":
    # if len(sys.argv) != 2:
    #     print("Usage: wordcount <file>", file=sys.stderr)
    #     sys.exit(-1)

    # Initialize Spark session and DataFrame
    spark, df = initialize()

    print("Count of dataset:" + str(df.count()))
    print("\nNumber of columns:" + str(len(df.columns)))

    print("\nDataset: \n")
    df.show(10)

    spark.stop()
