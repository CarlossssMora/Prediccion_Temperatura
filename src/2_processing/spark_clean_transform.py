from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, month, floor, rand,
    trim, initcap, regexp_replace, when
)
from pyspark.sql.types import DoubleType


# Rutas en HDFS
HDFS_INPUT = "/user/climate/raw/GlobalLandTemperaturesByCity.csv"
HDFS_OUTPUT = "/user/climate/processed/"


def compute_quartiles(df, column):
    """Devuelve Q1 y Q3 usando approxQuantile para eficiencia."""
    q1, q3 = df.approxQuantile(column, [0.25, 0.75], 0.01)
    return q1, q3


def main():

    # Configuración ajustada para máquinas personales
    spark = (
        SparkSession.builder
        .appName("ClimateDataProcessing")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.executor.memory", "2g")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )

    print("\n=== LEYENDO ARCHIVO DESDE HDFS ===\n")
    df = spark.read.csv(HDFS_INPUT, header=True)

    print(f"Filas cargadas: {df.count()}")

    # Conversión de tipos
    df = df.withColumn("dt", col("dt").cast("date"))
    df = df.withColumn("AverageTemperature", col("AverageTemperature").cast(DoubleType()))
    df = df.withColumn("AverageTemperatureUncertainty", col("AverageTemperatureUncertainty").cast(DoubleType()))

    # Columnas derivadas
    df = df.withColumn("year", year(col("dt")))
    df = df.withColumn("month", month(col("dt")))
    df = df.withColumn("decade", floor(col("year") / 10) * 10)

    # Estandarización del nombre de ciudad
    df = df.withColumn("City", initcap(trim(col("City"))))

    # Conversión de Latitude y Longitude a valores numéricos
    df = df.withColumn(
        "LatitudeNum",
        regexp_replace(col("Latitude"), "[A-Za-z]", "").cast("double")
    )
    df = df.withColumn(
        "LongitudeNum",
        regexp_replace(col("Longitude"), "[A-Za-z]", "").cast("double")
    )

    # Aplicar signo según hemisferio
    df = df.withColumn(
        "LatitudeNum",
        when(col("Latitude").endswith("S"), -col("LatitudeNum"))
        .otherwise(col("LatitudeNum"))
    )

    df = df.withColumn(
        "LongitudeNum",
        when(col("Longitude").endswith("W"), -col("LongitudeNum"))
        .otherwise(col("LongitudeNum"))
    )

    # Imputación basada en IQR
    print("\n=== CALCULANDO CUARTILES ===\n")

    temp_q1, temp_q3 = compute_quartiles(df, "AverageTemperature")
    unc_q1, unc_q3 = compute_quartiles(df, "AverageTemperatureUncertainty")

    print(f"AverageTemperature: Q1={temp_q1}, Q3={temp_q3}")
    print(f"AverageTemperatureUncertainty: Q1={unc_q1}, Q3={unc_q3}")

    # Imputación en valores nulos
    df = df.withColumn(
        "AverageTemperature",
        when(
            col("AverageTemperature").isNull(),
            rand() * (temp_q3 - temp_q1) + temp_q1
        ).otherwise(col("AverageTemperature"))
    )

    df = df.withColumn(
        "AverageTemperatureUncertainty",
        when(
            col("AverageTemperatureUncertainty").isNull(),
            rand() * (unc_q3 - unc_q1) + unc_q1
        ).otherwise(col("AverageTemperatureUncertainty"))
    )

    # Filtrar valores extremos
    df = df.filter(
        (col("AverageTemperature") > -50) &
        (col("AverageTemperature") < 60)
    )

    # Normalizar Country
    df = df.withColumn("Country", col("Country").cast("string"))

    # Reducción de particiones
    print("\n=== REDUCIENDO PARTICIONES PARA EVITAR ERRORES DE MEMORIA ===\n")
    df = df.repartition(4)   # <- perfecto para laptop 8–16 GB RAM

    print("\n=== ESQUEMA FINAL ===\n")
    df.printSchema()

    print("\n=== GUARDANDO EN PARQUET PARTICIONADO POR COUNTRY Y CITY ===\n")

    df.write.mode("overwrite") \
        .partitionBy("Country", "City") \
        .parquet(HDFS_OUTPUT)

    print("\nPROCESAMIENTO COMPLETADO\n")

    spark.stop()


if __name__ == "__main__":
    main()
