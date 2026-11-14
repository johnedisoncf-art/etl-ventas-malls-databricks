# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 2. Ingesta Bronze – Ventas (Auto Loader incremental)
# MAGIC Fuente: `raw/ventas/*.csv`. Se usa **Auto Loader** (`cloudFiles`) con `trigger(availableNow=True)`:
# MAGIC cada ejecución del workflow procesa solo los archivos nuevos que llegaron al contenedor raw.
# MAGIC El checkpoint vive en el contenedor bronze (ADLS), no en DBFS.

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("storageName", "adlsmallsjohn")
dbutils.widgets.text("container", "raw")
dbutils.widgets.text("catalogo", "catalog_dev")
dbutils.widgets.text("esquema", "bronze")

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType

storageName = dbutils.widgets.get("storageName")
container = dbutils.widgets.get("container")
catalogo = dbutils.widgets.get("catalogo")
esquema = dbutils.widgets.get("esquema")

ruta_origen = f"abfss://{container}@{storageName}.dfs.core.windows.net/ventas/"
checkpoint = f"abfss://bronze@{storageName}.dfs.core.windows.net/{catalogo}/_checkpoints/ventas"
tabla = f"{catalogo}.{esquema}.ventas"

# COMMAND ----------

# En bronze todo se guarda como texto: se conserva el dato original (incluidos errores) y se tipa en silver.
schema_ventas = StructType([StructField(c, StringType(), True) for c in [
    "id_venta", "fecha_venta", "id_tienda", "id_mall", "categoria",
    "cantidad", "monto_soles", "medio_pago"]])

df_stream = (spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", "csv")
             .option("header", True)
             .schema(schema_ventas)
             .load(ruta_origen)
             .withColumn("archivo_origen", col("_metadata.file_path"))
             .withColumn("fecha_ingesta", current_timestamp()))

# COMMAND ----------

filas_antes = spark.table(tabla).count()

query = (df_stream.writeStream
         .format("delta")
         .outputMode("append")
         .option("checkpointLocation", checkpoint)
         .trigger(availableNow=True)
         .toTable(tabla))

query.awaitTermination()

filas_despues = spark.table(tabla).count()
print(f"{tabla}: +{filas_despues - filas_antes} filas nuevas (total {filas_despues})")

# COMMAND ----------

display(spark.sql(f"SELECT archivo_origen, count(*) filas FROM {tabla} GROUP BY archivo_origen"))

# COMMAND ----------

dbutils.notebook.exit("OK")