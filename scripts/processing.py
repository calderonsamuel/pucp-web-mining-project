import polars as pl

ordenes_2024 = pl.read_csv('data/4_ordenes_de_servicio.txt', separator='\t', infer_schema=False).filter(
    pl.col("in_orden_anno") == "2024"
)
ordenes_2023 = pl.read_csv('data/4_ordenes_de_servicio_2023.txt', separator='\t', infer_schema=False)
ordenes_2022 = pl.read_csv('data/4_ordenes_de_servicio_2022.txt', separator='\t', infer_schema=False)
ordenes_2021 = pl.read_csv('data/4_ordenes_de_servicio_2021.txt', separator='\t', infer_schema=False)

ordenes = pl.concat([ordenes_2024, ordenes_2023, ordenes_2022, ordenes_2021])

ordenes.shape

ordenes.select(
    pl.col("in_orden_anno"),
    pl.col("in_orden_mes"),
    pl.col("entidad_nombre"),
    pl.col("dc_orden_monto"),
    pl.col("vc_orden_descripcion"),
).write_parquet('data/ordenes.parquet')

ordenes.unique("entidad_nombre").drop_in_place("entidad_nombre").sort().to_list()
