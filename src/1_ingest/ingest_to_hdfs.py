import os
import subprocess
import pandas as pd

# Ruta local del CSV
LOCAL_PATH = "/home/carlos-mora/Documentos/IPN/6to_Semestre/Big_Data/programas/proyecto/data/raw/GlobalLandTemperaturesByCity.csv"

# Ruta destino en HDFS
HDFS_DIR = "/user/climate/raw/"
HDFS_FILE = HDFS_DIR + "GlobalLandTemperaturesByCity.csv"


def check_local_file():
    if not os.path.exists(LOCAL_PATH):
        raise FileNotFoundError(f"No se encontró el archivo en {LOCAL_PATH}")
    print(f"Archivo encontrado: {LOCAL_PATH}")


def validate_csv_columns():
    df = pd.read_csv(LOCAL_PATH, nrows=5)
    required_cols = [
        "dt",
        "AverageTemperature",
        "AverageTemperatureUncertainty",
        "City",
        "Country",
        "Latitude",
        "Longitude",
    ]

    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Falta la columna requerida: {col}")

    print("Columnas validadas correctamente")


def run_hdfs_command(command):
    """Ejecuta un comando HDFS desde Python."""
    full_cmd = ["hdfs", "dfs"] + command
    print("Ejecutando:", " ".join(full_cmd))
    subprocess.run(full_cmd, check=True)


def create_hdfs_dirs():
    print(f"Verificando carpeta HDFS: {HDFS_DIR}")
    run_hdfs_command(["-mkdir", "-p", HDFS_DIR])
    print(f"Carpeta creada o ya existente: {HDFS_DIR}")


def upload_to_hdfs():
    print("Subiendo archivo a HDFS...")
    run_hdfs_command(["-put", "-f", LOCAL_PATH, HDFS_FILE])
    print(f"Archivo subido a HDFS en: {HDFS_FILE}")


def main():
    print("\n===== INGESTA DE DATOS A HDFS =====\n")

    check_local_file()
    validate_csv_columns()
    create_hdfs_dirs()
    upload_to_hdfs()

    print("\nINGESTA COMPLETADA CON ÉXITO\n")


if __name__ == "__main__":
    main()
