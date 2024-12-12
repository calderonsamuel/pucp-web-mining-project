import polars as pl
import numpy as np

ordenes = pl.read_parquet('data/ordenes_servicio_lda_final_v4.parquet')

ordenes.columns

# Select and rename columns
ordenes_base = ordenes.with_columns(
        pl.when(pl.col("fk_id_orden_tipo") == 1).then(pl.lit("Compra")).otherwise(pl.lit("Servicio")).alias("Tipo"),
    ).select(
    pl.col("pk_id_orden").alias("ID"),
    pl.col("Tipo"),
    pl.col("vc_orden_numero").alias("Numero"),
    pl.col("in_orden_anno").alias("Anno"),
    pl.col("in_orden_mes").alias("Mes"),
    pl.col("entidad_nombre").alias("Entidad"),
    pl.col("vc_orden_descripcion").alias("Descripcion"),
    pl.col("dc_orden_monto").cast(pl.Float64).alias("Monto"),
)

ordenes_lda = ordenes.select(
    pl.col("pk_id_orden").alias("ID"),
    pl.col('^rubro_.*$')
)

ordenes_full = ordenes.rename({
    "pk_id_orden": "ID",
    "fk_id_orden_tipo": "Tipo",
    "vc_orden_numero": "Numero",
    "in_orden_anno": "Anno",
    "in_orden_mes": "Mes",
    "entidad_nombre": "Entidad",
    "vc_orden_descripcion": "Descripcion",
    "dc_orden_monto": "Monto",
}).with_columns(
        pl.when(pl.col("Tipo") == 1).then(pl.lit("Compra")).otherwise(pl.lit("Servicio")).alias("Tipo"),
    )


ordenes_base.write_parquet('data/ordenes.parquet')
ordenes_lda.write_parquet('data/ordenes_lda.parquet')
ordenes_full.write_parquet('data/ordenes_full.parquet')

ordenes_full.group_by(
    pl.col("rubro_asignado"),
    pl.col("palabras_clave")
).count().with_columns(
    pl.col("palabras_clave").str.split(", ").alias("palabras_clave")
).explode("palabras_clave")

keywords = ordenes_full.unique(["rubro_asignado", "palabras_clave"]).select(
    pl.col("rubro_asignado"),
    pl.col("palabras_clave")
)

keywords.write_json('data/keywords.json')

# logic to  get the most similar orders

ordenes_sample = ordenes.sample(100)
vector = pl.Series(
    'vector',
    [0.03333546221256256, 0.03333546221256256, 0.03333546221256256, 0.03333546221256256, 0.03334055468440056, 0.03333546221256256, 0.033355146646499634, 0.03333546221256256, 0.6999560594558716, 0.03333546221256256]
)

def euclidean_distance(arr1, arr2):
        return np.sqrt(np.sum((np.array(arr1) - np.array(arr2)) ** 2))

# def compute_distance(v1: pl.Series, v2: pl.Series) -> pl.Series:
#     # Apply the function element-wise to the Series
#     return pl.Series([euclidean_distance(a, b) for a, b in zip(v1, v2)])

def compute_distance(v1: pl.Series, v2: pl.Series) -> pl.Series:
    # Ensure v2 is of length 1 and extend it to match v1's length
    v2_extended = [v2[0]] * len(v1)
    
    # Compute distances
    distances = [euclidean_distance(a, b) for a, b in zip(v1, v2_extended)]
    
    return pl.Series('distances', distances)

v1 = pl.Series('v1', [[0, 0]])
v2 = pl.Series('v2', [[0, 2]])

compute_distance(v1, v2).to_list() # [2.0]

v1 = pl.Series('v1', [[0, 0], [0, 0]])
v2 = pl.Series('v2', [[0, 2]])

compute_distance(v1, v2).to_list() # [2.0, 2.0]

# this works
ordenes_sample.with_columns(
    compute_distance(ordenes_sample["vector_probabilidades"], vector).alias("distancia")
).select(pl.col("distancia"))


# generate plots

import plotly.express as px

data_sum = ordenes_full.select(
      pl.col("rubro_asignado"),
      pl.col("Monto")
).group_by("rubro_asignado").sum().sort("rubro_asignado")

fig = px.bar(data_sum, x='rubro_asignado', y='Monto')
fig.show()

ordenes_full.select(
      pl.col("rubro_asignado"),
      pl.col("Monto")
).group_by("rubro_asignado").count().sort("rubro_asignado")
