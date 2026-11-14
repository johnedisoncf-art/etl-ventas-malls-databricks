# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 2. Ingesta Bronze – Centros comerciales (CSV)
# MAGIC Fuente: `raw/maestros/centros_comerciales.csv` en ADLS (acceso vía External Location / Managed Identity).

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("storageName", "adlsmallsjohn")
dbutils.widgets.text("container", "raw")
dbutils.widgets.text("catalogo", "catalog_dev")
dbutils.widgets.text("esquema", "bronze")

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

storageName = dbutils.widgets.get("storageName")
container = dbutils.widgets.get("container")
catalogo = dbutils.widgets.get("catalogo")
esquema = dbutils.widgets.get("esquema")

ruta = f"abfss://{container}@{storageName}.dfs.core.windows.net/maestros/centros_comerciales.csv"
tabla = f"{catalogo}.{esquema}.centros_comerciales"

# COMMAND ----------

schema_malls = StructType([
    StructField("id_mall", IntegerType(), False),
    StructField("nombre_mall", StringType(), True),
    StructField("operador", StringType(), True),
    StructField("distrito", StringType(), True),
    StructField("area_m2", IntegerType(), True),
    StructField("anio_apertura", IntegerType(), True),
])

df_malls = (spark.read
            .option("header", True)
            .schema(schema_malls)
            .csv(ruta))

display(df_malls)

# COMMAND ----------

df_final = df_malls.withColumn("fecha_ingesta", current_timestamp())

# insertInto es posicional: se alinean las columnas con la tabla destino
df_final.select(*spark.table(tabla).columns).write.mode("overwrite").insertInto(tabla)

print(f"{tabla}: {spark.table(tabla).count()} filas")

# COMMAND ----------

dbutils.notebook.exit("OK")