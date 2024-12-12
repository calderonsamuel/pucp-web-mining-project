import polars as pl

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
