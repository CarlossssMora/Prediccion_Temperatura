import streamlit as st
import pandas as pd
import plotly.express as px
import numpy as np

st.set_page_config(layout="wide")
st.title(" Climate Temperature Dashboard")

# =============================
# Cargar datos locales
# =============================
@st.cache_data
def load_data():
    preds = pd.read_parquet("data/dashboard/model_output/predictions")
    metrics = pd.read_parquet("data/dashboard/model_output/metrics")
    return preds, metrics


preds, metrics = load_data()

# =============================
# MÉTRICAS
# =============================
st.subheader(" Métricas del Modelo")

c1, c2, c3 = st.columns(3)
c1.metric("RMSE", round(metrics["RMSE"][0], 3))
c2.metric("MAE", round(metrics["MAE"][0], 3))
c3.metric("R²", round(metrics["R2"][0], 3))

# =============================
# Predicción vs Real (colores distintos)
# =============================
st.subheader("Predicción vs Valor Real")

sample = preds.sample(5000, random_state=42)

# Convertimos a formato largo
plot_df = pd.DataFrame({
    "Temperatura": pd.concat([sample["label"], sample["prediction"]]),
    "Tipo": ["Real"] * len(sample) + ["Predicha"] * len(sample),
    "Referencia": pd.concat([sample["label"], sample["label"]])
})

fig = px.scatter(
    plot_df,
    x="Referencia",
    y="Temperatura",
    color="Tipo",
    opacity=0.5,
    color_discrete_map={
        "Real": "#1f77b4",      # azul
        "Predicha": "#ff7f0e"   # naranja
    },
    labels={
        "Referencia": "Temperatura real",
        "Temperatura": "Temperatura",
        "Tipo": "Dato"
    },
    title="Temperaturas Reales vs Predichas"
)



st.plotly_chart(fig, use_container_width=True)

# =============================
# 2. Tendencia global 1750–2013
# =============================
st.subheader(" Tendencia Global de Temperatura")

global_trend = (
    preds.groupby("year")["label"]
    .mean()
    .reset_index()
)

fig2 = px.line(
    global_trend,
    x="year",
    y="label",
    labels={"label": "Temperatura promedio"},
    title="Temperatura Global Promedio por Año"
)

st.plotly_chart(fig2, use_container_width=True)

# =============================
# 3. Tendencia por país
# =============================
st.subheader(" Tendencia por País")

country = st.selectbox(
    "Selecciona un país:",
    sorted(preds["Country"].dropna().unique())
)

country_df = (
    preds[preds["Country"] == country]
    .groupby("year")["label"]
    .mean()
    .reset_index()
)

fig3 = px.line(
    country_df,
    x="year",
    y="label",
    title=f"Tendencia de Temperatura en {country}"
)

st.plotly_chart(fig3, use_container_width=True)

# =============================
# Predicción futura global
# =============================
st.subheader(" Predicción Global Futura (2030–2050)")

future = pd.DataFrame({
    "year": range(2030, 2051),
    "month": 6,
})

future["decade"] = (future["year"] // 10) * 10
future["LatitudeNum"] = preds["LatitudeNum"].mean()
future["LongitudeNum"] = preds["LongitudeNum"].mean()

# Aproximación por tendencia global
coef = global_trend["label"].diff().mean()

future["prediction"] = (
    global_trend["label"].iloc[-1]
    + coef * (future["year"] - global_trend["year"].max())
)

fig_future = px.line(
    future,
    x="year",
    y="prediction",
    title="Predicción Global de Temperatura (2030–2050)",
    labels={"prediction": "Temperatura estimada (°C)"}
)

st.plotly_chart(fig_future, use_container_width=True)



st.subheader(" Mapa de Calor Global (Temperatura Promedio)")

heat_df = (
    preds.groupby(["LatitudeNum", "LongitudeNum"])["label"]
    .mean()
    .reset_index()
)

fig_map = px.density_mapbox(
    heat_df,
    lat="LatitudeNum",
    lon="LongitudeNum",
    z="label",
    radius=10,
    center=dict(lat=0, lon=0),
    zoom=0,
    mapbox_style="carto-positron",
    title="Distribución Global de Temperaturas"
)

st.plotly_chart(fig_map, use_container_width=True)



st.subheader(" Error del Modelo vs Temperatura Real")

preds["error"] = preds["prediction"] - preds["label"]

sample_err = preds.sample(5000, random_state=1)

fig_err = px.scatter(
    sample_err,
    x="label",
    y="error",
    opacity=0.4,
    labels={
        "label": "Temperatura real",
        "error": "Error (pred - real)"
    },
    title="Error del Modelo vs Temperatura Real"
)

st.plotly_chart(fig_err, use_container_width=True)



st.subheader(" Comparación por Hemisferio")

preds["Hemisferio"] = np.where(
    preds["LatitudeNum"] >= 0,
    "Norte",
    "Sur"
)

hemi_df = (
    preds.groupby(["year", "Hemisferio"])["label"]
    .mean()
    .reset_index()
)

fig_hemi = px.line(
    hemi_df,
    x="year",
    y="label",
    color="Hemisferio",
    title="Evolución de Temperatura por Hemisferio"
)

st.plotly_chart(fig_hemi, use_container_width=True)



st.success(" Dashboard completo")
