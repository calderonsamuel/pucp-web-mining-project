import polars as pl
import duckdb

ordenes_2024 = pl.read_csv('data/4_ordenes_de_servicio.txt', separator='\t', infer_schema=False).filter(
    pl.col("in_orden_anno") == "2024"
)
ordenes_2023 = pl.read_csv('data/4_ordenes_de_servicio_2023.txt', separator='\t', infer_schema=False)
ordenes_2022 = pl.read_csv('data/4_ordenes_de_servicio_2022.txt', separator='\t', infer_schema=False)
ordenes_2021 = pl.read_csv('data/4_ordenes_de_servicio_2021.txt', separator='\t', infer_schema=False)

ordenes = pl.concat([ordenes_2024, ordenes_2023, ordenes_2022, ordenes_2021])

# Select and rename columns
ordenes_selected = ordenes.select(
    pl.col("pk_id_orden").alias("ID"),
    pl.col("fk_id_orden_tipo").alias("Tipo"),
    pl.col("vc_orden_numero").alias("Numero"),
    pl.col("in_orden_anno").alias("Anno"),
    pl.col("in_orden_mes").alias("Mes"),
    pl.col("entidad_nombre").alias("Entidad"),
    pl.col("vc_orden_descripcion").alias("Descripcion"),
    pl.col("dc_orden_monto").alias("Monto"),
)

ordenes_selected.write_parquet('data/ordenes.parquet')

# Convert Polars DataFrame to Pandas DataFrame
ordenes_selected_pd = ordenes_selected.to_pandas()

# Write to DuckDB file
con = duckdb.connect('data/ordenes.duckdb')
con.execute("CREATE OR REPLACE TABLE ordenes AS SELECT * FROM ordenes_selected_pd")
con.close()
