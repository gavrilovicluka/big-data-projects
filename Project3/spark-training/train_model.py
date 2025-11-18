import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression, GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml import Pipeline


APP_NAME = "FlightDelayPretrain"

# -------------------------------
# ENV variables
# -------------------------------
DATA_PATH = os.getenv("DATA_PATH", "./data/full_data_flightdelay.csv")
MODEL_PATH = os.getenv("MODEL_PATH", "/model/lr_pipeline_model")


# -------------------------------
# Feature engineering columns
# -------------------------------
NUMERIC_FEATURES = [
    "MONTH", "DAY_OF_WEEK", "DISTANCE_GROUP",
    "CONCURRENT_FLIGHTS", "AIRPORT_FLIGHTS_MONTH",
    "AIRLINE_FLIGHTS_MONTH", "AVG_MONTHLY_PASS_AIRPORT", 
    "AVG_MONTHLY_PASS_AIRLINE", "PLANE_AGE",
    "TMAX", "AWND", "LATITUDE", "LONGITUDE"
]
CATEGORICAL_FEATURES = ["CARRIER_NAME", "DEPARTING_AIRPORT"]
TARGET_COL = "DEP_DEL15"
INDEXED_FEATURES = [f"{c}_indexed" for c in CATEGORICAL_FEATURES]
FEATURE_VECTOR = "features_unscaled"
SCALED_VECTOR = "features"


# -------------------------------
# Spark session
# -------------------------------
def initialize_spark():
    spark = SparkSession.builder \
        .appName(APP_NAME) \
        .getOrCreate()
    
    print("[model-training] Current Spark master:", spark.sparkContext.master)

    spark.sparkContext.setLogLevel("ERROR")

    print("[model-training] Spark session initialized.")
    return spark


def load_data(spark):
    print(f"Loading data from: {DATA_PATH}...")
    df = spark.read.csv(DATA_PATH, header=True, inferSchema=True)

    for c in NUMERIC_FEATURES:
        df = df.withColumn(c, col(c).cast("double"))
        df = df.na.fill(0.0, subset=[c])

    df = df.na.drop(subset=[TARGET_COL])

    df = df.withColumn(TARGET_COL, col(TARGET_COL).cast("integer"))

    print("\n[model-training] Columns in dataset:", df.columns)
    print(f"\n[model-training] Dataset rows count: {df.count()}")
    return df


# -------------------------------
# ML Pipeline
# -------------------------------
def define_pipeline():
    # String Indexer for categorical features
    indexers = [
        StringIndexer(inputCol=c, outputCol=f"{c}_indexed", handleInvalid="keep")
        for c in CATEGORICAL_FEATURES
    ]

    # Vector Assembler for merging all features
    assembler_inputs = NUMERIC_FEATURES + INDEXED_FEATURES
    assembler = VectorAssembler(inputCols=assembler_inputs, outputCol=FEATURE_VECTOR, handleInvalid="skip")

    # StandardScaler for scaling
    scaler = StandardScaler(inputCol=FEATURE_VECTOR, outputCol=SCALED_VECTOR, withStd=True, withMean=True)

    # Logistic Regression Model
    # lr = LogisticRegression(featuresCol=SCALED_VECTOR, labelCol=TARGET_COL, maxIter=10)
    # rf = RandomForestClassifier(featuresCol=SCALED_VECTOR, labelCol=TARGET_COL, numTrees=20)
    # rf = RandomForestClassifier(featuresCol=SCALED_VECTOR, labelCol=TARGET_COL, numTrees=10, maxDepth=5)
    gbt = GBTClassifier(featuresCol="features", labelCol=TARGET_COL, maxIter=10, maxDepth=5)

    # Pipeline
    pipeline = Pipeline(stages=indexers + [assembler, scaler, gbt])
    return pipeline


# -------------------------------
# Model training and saving
# -------------------------------
def train_and_save_model(df, pipeline):
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

    minority_df = train_df.filter(col(TARGET_COL) == 1)
    majority_df = train_df.filter(col(TARGET_COL) == 0)
    minority_count = minority_df.count()

    majority_sample_ratio = minority_count * 3 / majority_df.count()
    
    majority_sampled_df = majority_df.sample(False, majority_sample_ratio, seed=42)

    train_balanced_df = majority_sampled_df.union(minority_df)

    print(f"[model-training] Balance training 1: {round(1/majority_sample_ratio)} (Total rows: {train_balanced_df.count()})")

    print("[model-training] Starting ML Pipeline training...")
    pipeline_model = pipeline.fit(train_balanced_df)

    predictions = pipeline_model.transform(test_df)

    OPTIMAL_THRESHOLD = 0.25
    gbt_model = pipeline_model.stages[-1]
    gbt_model.setThresholds([1 - OPTIMAL_THRESHOLD, OPTIMAL_THRESHOLD])

    print(f"\n[model-training] GBT 'Delay' prediction threshold set to: {OPTIMAL_THRESHOLD}")

    predictions_tuned = pipeline_model.transform(test_df)

    # Evaluate
    evaluator_auc = BinaryClassificationEvaluator(labelCol=TARGET_COL, rawPredictionCol="rawPrediction", metricName="areaUnderROC")
    auc = evaluator_auc.evaluate(predictions_tuned)
    print(f"\n[model-training] ----- AUC (Area Under ROC) on test set: {auc:.4f} -----")

    evaluator_f1 = MulticlassClassificationEvaluator(labelCol=TARGET_COL, predictionCol="prediction", metricName="f1")
    f1 = evaluator_f1.evaluate(predictions_tuned)
    print(f"\n[model-training] ----- F1-score on test set: {f1:.4f}  -----")

    # Saving model
    pipeline_model.write().overwrite().save(MODEL_PATH)
    print(f"\n[model-training] ----- Pipeline saved to: {MODEL_PATH} -----")


# -------------------------------
# Main
# -------------------------------
if __name__ == '__main__':
    spark = initialize_spark()
    df_raw = load_data(spark)
    pipeline = define_pipeline()
    train_and_save_model(df_raw, pipeline)
    spark.stop()