from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, min, max, count
)

# Ruta de entrada (carpeta con Parquets procesados)
HDFS_INPUT = "/user/climate/processed/"


def main():

    spark = (
        SparkSession.builder
        .appName("ClimateEDA")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    print("\n=== LEYENDO PARQUET DESDE HDFS ===\n")
    df = spark.read.parquet(HDFS_INPUT)

    print(f"Filas procesadas: {df.count()}\n")
    df.printSchema()

    # ============================================================
    # 1. ESTADÍSTICAS GLOBALES
    # ============================================================

    print("\n=== ESTADÍSTICAS GLOBALES ===\n")
    global_stats = df.select(
        avg("AverageTemperature").alias("avg_temp"),
        min("AverageTemperature").alias("min_temp"),
        max("AverageTemperature").alias("max_temp"),
        count("*").alias("total_registros")
    )
    global_stats.show(truncate=False)

    # ============================================================
    # 2. TEMPERATURA PROMEDIO POR PAÍS
    # ============================================================

    print("\n=== TEMPERATURA PROMEDIO POR PAÍS ===\n")
    temp_by_country = (
        df.groupBy("Country")
        .agg(avg("AverageTemperature").alias("avg_temp"))
        .orderBy(col("avg_temp").desc())
    )
    temp_by_country.show(20, truncate=False)

    # ============================================================
    # 3. TOP 10 PAÍSES MÁS CALIENTES
    # ============================================================

    print("\n=== TOP 10 PAÍSES MÁS CALIENTES ===\n")
    top10_countries = temp_by_country.limit(10)
    top10_countries.show(truncate=False)

    # ============================================================
    # 4. EVOLUCIÓN GLOBAL POR AÑO
    # ============================================================

    print("\n=== PROMEDIO GLOBAL POR AÑO ===\n")
    global_trend = (
        df.groupBy("year")
        .agg(avg("AverageTemperature").alias("avg_temp"))
        .orderBy("year")
    )
    global_trend.show(50, truncate=False)

    global_trend.write.mode("overwrite").parquet("/user/climate/output/global_trend/")

    # ============================================================
    # 5. REGISTROS POR PAÍS
    # ============================================================

    print("\n=== CANTIDAD DE REGISTROS POR PAÍS ===\n")
    count_countries = (
        df.groupBy("Country")
        .agg(count("*").alias("count"))
        .orderBy(col("count").desc())
    )
    count_countries.show(20, truncate=False)

    count_countries.write.mode("overwrite").parquet("/user/climate/output/counts_by_country/")

    # ============================================================
    # 6. TEMPERATURA PROMEDIO POR CIUDAD
    # ============================================================

    print("\n=== TEMPERATURA PROMEDIO POR CIUDAD ===\n")
    temp_by_city = (
        df.groupBy("City")
        .agg(avg("AverageTemperature").alias("avg_temp"))
        .orderBy(col("avg_temp").desc())
    )
    temp_by_city.show(20, truncate=False)

    temp_by_city.write.mode("overwrite").parquet("/user/climate/output/temp_by_city/")

    # ============================================================
    # 7. TOP 10 CIUDADES MÁS CALIENTES
    # ============================================================

    print("\n=== TOP 10 CIUDADES MÁS CALIENTES ===\n")
    top10_cities = temp_by_city.limit(10)
    top10_cities.show(truncate=False)

    top10_cities.write.mode("overwrite").parquet("/user/climate/output/top10_cities/")

    # ============================================================
    # 8. REGISTROS POR CIUDAD
    # ============================================================

    print("\n=== CANTIDAD DE REGISTROS POR CIUDAD ===\n")
    count_cities = (
        df.groupBy("City")
        .agg(count("*").alias("count"))
        .orderBy(col("count").desc())
    )
    count_cities.show(20, truncate=False)

    count_cities.write.mode("overwrite").parquet("/user/climate/output/counts_by_city/")

    # ============================================================
    # 9. SERIES TEMPORALES POR PAÍS (EJEMPLO: México)
    # ============================================================

    print("\n=== SERIE TEMPORAL PARA MÉXICO ===\n")
    mexico_trend = (
        df.filter(col("Country") == "Mexico")
        .groupBy("year")
        .agg(avg("AverageTemperature").alias("avg_temp"))
        .orderBy("year")
    )
    mexico_trend.show(50, truncate=False)

    mexico_trend.write.mode("overwrite").parquet("/user/climate/output/mexico_trend/")

    # ============================================================
    # 10. SERIES TEMPORALES POR CIUDAD (EJEMPLO: Guadalajara)
    # ============================================================

    print("\n=== SERIE TEMPORAL PARA GUADALAJARA ===\n")
    guadalajara_trend = (
        df.filter(col("City") == "Guadalajara")
        .groupBy("year")
        .agg(avg("AverageTemperature").alias("avg_temp"))
        .orderBy("year")
    )
    guadalajara_trend.show(50, truncate=False)

    guadalajara_trend.write.mode("overwrite").parquet("/user/climate/output/guadalajara_trend/")

    # FINAL
    print("\nEDA COMPLETADO\n")

    spark.stop()


if __name__ == "__main__":
    main()
