from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

# ==========================
# Rutas HDFS
# ==========================
HDFS_INPUT = "/user/climate/processed/"
HDFS_OUTPUT = "/user/climate/model_output/"

def main():

    spark = (
        SparkSession.builder
        .appName("ClimateTemperatureForecast_LinearNonLinearFeatures")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )

    print("\n=== CARGANDO DATOS PROCESADOS ===\n")
    df = spark.read.parquet(HDFS_INPUT)

    print(f"Total de registros: {df.count()}")

    # ==========================
    # CREACIÓN DE FEATURES NO LINEALES
    # ==========================
    df = (
        df
        .withColumn("year2", col("year") * col("year"))
        .withColumn("lat2", col("LatitudeNum") * col("LatitudeNum"))
        .withColumn("lon2", col("LongitudeNum") * col("LongitudeNum"))
        .withColumn("year_lat", col("year") * col("LatitudeNum"))
        .withColumn("month_lat", col("month") * col("LatitudeNum"))
    )

    # ==========================
    # Selección final
    # ==========================
    df = df.select(
        col("AverageTemperature").alias("label"),
        "year", "year2",
        "month",
        "decade",
        "LatitudeNum", "lat2",
        "LongitudeNum", "lon2",
        "year_lat",
        "month_lat",
        "Country",
        "City"
    ).dropna()

    # ==========================
    # Vector de características
    # ==========================
    features = [
        "year", "year2",
        "month",
        "decade",
        "LatitudeNum", "lat2",
        "LongitudeNum", "lon2",
        "year_lat",
        "month_lat"
    ]

    assembler = VectorAssembler(
        inputCols=features,
        outputCol="features"
    )

    df = assembler.transform(df)

    # ==========================
    # Train / Test split
    # ==========================
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

    print(f"Train: {train_df.count()} | Test: {test_df.count()}")

    # ==========================
    # MODELO: REGRESIÓN LINEAL
    # ==========================
    lr = LinearRegression(
        featuresCol="features",
        labelCol="label",
        maxIter=30,
        regParam=0.1,        # Regularización (MUY importante)
        elasticNetParam=0.5 # Ridge + Lasso
    )

    model = lr.fit(train_df)

    # ==========================
    # Predicciones
    # ==========================
    predictions = model.transform(test_df).select(
        "label",
        "prediction",
        "year",
        "month",
        "decade",
        "LatitudeNum",
        "LongitudeNum",
        "Country",
        "City"
    )

    # ==========================
    # Evaluación
    # ==========================
    evaluator = RegressionEvaluator(
        labelCol="label",
        predictionCol="prediction"
    )

    rmse = evaluator.setMetricName("rmse").evaluate(predictions)
    mae  = evaluator.setMetricName("mae").evaluate(predictions)
    r2   = evaluator.setMetricName("r2").evaluate(predictions)

    print("\n=== MÉTRICAS DEL MODELO (Linear + No Lineal) ===")
    print(f"RMSE: {rmse}")
    print(f"MAE : {mae}")
    print(f"R2  : {r2}")

    # ==========================
    # Guardar resultados
    # ==========================
    predictions.write.mode("overwrite").parquet(
        HDFS_OUTPUT + "predictions/"
    )

    metrics_df = spark.createDataFrame(
        [(rmse, mae, r2)],
        ["RMSE", "MAE", "R2"]
    )

    metrics_df.write.mode("overwrite").parquet(
        HDFS_OUTPUT + "metrics/"
    )

    print("\nMODELO LINEAL CON FEATURES NO LINEALES COMPLETADO\n")

    spark.stop()

if __name__ == "__main__":
    main()
