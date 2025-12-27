# Análisis del Cambio Climático y Predicción de Temperatura utilizando Big Data
Este proyecto construye un pipeline completo de Big Data para analizar la evolución de la temperatura global utilizando el dataset **GlobalLandTemperaturesByCity.csv**, proveniente del Berkeley Earth Surface Temperature Study y publicado en Kaggle.

El objetivo es demostrar una solución integral que incluya:

- Ingesta y procesamiento inicial en un entorno distribuido  
- Limpieza, transformación y almacenamiento en formato óptimo  
- Análisis exploratorio a gran escala  
- Modelo predictivo con SparkML  
- Visualización interactiva mediante un dashboard  

---

# Estructura del repositorio
<img width="550" height="552" alt="image" src="https://github.com/user-attachments/assets/9071f1ae-5236-46e8-b0bf-5823fc833371" />


---

# Dataset utilizado

**Nombre:** Global Land Temperatures by City  
**Fuente:** Kaggle  
**Basado en:** Berkeley Earth Surface Temperature Study  
**Filas aproximadas:** ~8.6 millones  
**Rango temporal:** 1743 – 2013  
**Columnas principales:**
- `dt` — Fecha  
- `AverageTemperature` — Temperatura promedio en tierra en grados Celsius
- `AverageTemperatureUncertainty` — 
- `City` — Ciudad
- `Country` — País
- `Latitude`, `Longitude` — Latitud y Longitud, respectivamente

Este dataset contiene **temperaturas mensuales históricas** para miles de ciudades alrededor del mundo.

---

# Arquitectura del Pipeline

El pipeline tiene cinco etapas principales:

---

# Uso de entorno virtual
Para ejecutar este proyecto es necesario utilizar el entorno virtual preparado con el nombre **climate-env**, el cual ya contiene las bibliotecas necesarias para la ejecución de los scripts.

## Activar el entorno virtual

### Linux / macOS
```bash
source climate-env/bin/activate
```

### Windows
```bash
climate-env\Scripts\Activate.ps1
```

---

# Ejecución del pipeline
A continuación se muestran los comandos a ejecutar una vez que se haya activado el entorno virtual. Cabe recalcar que el orden en que se presentan los comandos es como se debe de ejecutar los scripts de este proyecto.

Antes de ejecutar los scripts, es importante inicializar los demonios de HDFS, de lo contrario no funcionará el proyecto.
```bash
start-dfs.sh
start-yarn.sh
```

## 1. Ingesta del CSV al HDFS
Este scipt está encargado de leer el archivo CSV, valida las columnas que contiene el archivo y subirlo al hdfs en la ruta ```/user/climate/raw/```.

Ejecutar con:
```bash
python /tu_ruta_local_al_proyecto/src/1_ingest/ingest_to_hdfs.py
```

## 2. Limpieza y transformación con Spark
En este proceso se realiza la normalización de valores de todos los campos, tratamiento de valores nulos, creación de columnas derivadas respecto al año, mes y década, y generar el particionado del parquet de la forma ```data/processed/year=YYYY/country=pais/city=ciudad```.

Ejecutar con:
```bash
spark-submit /tu_ruta_local_al_proyecto/src/2_processing/spark_clean_transform.py
```

## 3. Análisis exploratorio distribuido
Al momento de ejecutar este script se obtiene estadísticas globales de las temperaturas, temperatura promedio por país, top 10 países con las temperaturas más altas, entre otros análisis que están implementados dentro de la biblioteca de SparkML.

Ejecutar con:
```bash
spark-submit /tu_ruta_local_al_proyecto/src/3_analysis/spark_eda.py
```

## 4. Entrenamiento del modelo predictivo
En este programa se entrenan los modelos para la predicción de la temperatura global y el autoentrenamiento de los modelos para la predicción de temperaturas por país, además de mostrar las métricas (RMSE, MAE, R²), curvas de predicción y dataset final de predicciones (Parquet).

Ejecutar con:
```bash
spark-submit /tu_ruta_local_al_proyecto/src/4_model/spark_temperature_forecast.py
```

## 5. Dashboard interactivo
Finalmente, se despliega una visualización de gráficas que toman como base los datos y las predicciones obtenidas del scipt anterior. Incluye:
- Mapas de calor globales
- Gráfica 1750–2013 de temperatura
- Tendencias por ciudad/país
- Predicción futura (2030–2050)
- Entre otras

Para esta etapa, lo primero que se debe de hacer es ejecutar la etapa 4, una vez que se termina de ejecutar, en una terminal dentro de la carpeta del proyecto pero fuera de los entornos virtuales, lo que se debe hacer es comprobar que al poner este comando:
```bash
hdfs dfs -ls /user/climate/model_output
```
Se pueda ver:
```bash
predictions/
metrics/
```

Luego, se deben copiar los resultados obtenidos que se encuentran en HDFS a local:
```bash
hdfs dfs -get /user/climate/model_output data/dashboard/
```

Verificar que se encuenten predictions y metrics
```bash
ls data/dashboard/model_output
```
Activar el entorno virtual con:
```bash
source dashboard-env/bin/activate
```


Ejecutar con:
```bash
streamlit run src/5_visualization/dashboard.py

```
