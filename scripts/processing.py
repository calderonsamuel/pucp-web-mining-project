import polars as pl

ordenes = pl.read_csv('data/4_ordenes_de_servicio.txt', separator='\t', infer_schema=False)

print(ordenes.columns)

ordenes.group_by(
    pl.col("in_orden_anno"),
    pl.col("entidad_id"),
    pl.col("entidad_nombre")
)

ordenes.shape

ordenes.write_parquet('data/ordenes.parquet') 
