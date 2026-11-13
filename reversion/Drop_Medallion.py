# Databricks notebook source
# MAGIC %md
# MAGIC # Reversión – eliminar tablas, datos y checkpoints del ambiente
# MAGIC Úsalo para volver a cargar desde cero (por ejemplo, en dev). **No forma parte del workflow de producción.**

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("storageName", "adlsmallsdata")
dbutils.widgets.text("catalogo", "catalog_dev")
dbutils.widgets.dropdown("eliminarCatalogo", "no", ["no", "si"])

# COMMAND ----------

storageName = dbutils.widgets.get("storageName")
catalogo = dbutils.widgets.get("catalogo")

tablas = {
    "bronze": ["centros_comerciales", "tiendas", "ventas", "afluencia"],
    "silver": ["dim_mall", "dim_tienda", "fact_ventas", "afluencia_diaria"],
    "golden": ["ventas_mensuales_mall", "ranking_tiendas", "conversion_diaria_mall", "ventas_rubro_medio_pago"],
}

for capa, lista in tablas.items():
    for t in lista:
        spark.sql(f"DROP TABLE IF EXISTS {catalogo}.{capa}.{t}")
        # Las tablas son EXTERNAS: DROP no borra los archivos, se eliminan explícitamente
        dbutils.fs.rm(f"abfss://{capa}@{storageName}.dfs.core.windows.net/{catalogo}/{t}", True)
        print(f"Eliminada {catalogo}.{capa}.{t}")

# checkpoint de Auto Loader (si no se borra, no se re-ingieren los archivos ya leídos)
dbutils.fs.rm(f"abfss://bronze@{storageName}.dfs.core.windows.net/{catalogo}/_checkpoints", True)

# COMMAND ----------

if dbutils.widgets.get("eliminarCatalogo") == "si":
    spark.sql(f"DROP CATALOG IF EXISTS {catalogo} CASCADE")
    print(f"Catálogo {catalogo} eliminado")
