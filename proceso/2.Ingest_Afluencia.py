# Databricks notebook source
# MAGIC %md
# MAGIC # 2. Ingesta Bronze – Afluencia de visitantes (CSV)
# MAGIC Fuente: `raw/afluencia/afluencia_visitantes.csv` (visitantes diarios por centro comercial).

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("storageName", "adlsmallsjohn")
dbutils.widgets.text("container", "raw")
dbutils.widgets.text("catalogo", "catalog_dev")
dbutils.widgets.text("esquema", "bronze")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, DateType

storageName = dbutils.widgets.get("storageName")
container = dbutils.widgets.get("container")
catalogo = dbutils.widgets.get("catalogo")
esquema = dbutils.widgets.get("esquema")

ruta = f"abfss://{container}@{storageName}.dfs.core.windows.net/afluencia/"
tabla = f"{catalogo}.{esquema}.afluencia"

# COMMAND ----------

schema_afluencia = StructType([
    StructField("fecha", DateType(), True),
    StructField("id_mall", IntegerType(), True),
    StructField("visitantes", IntegerType(), True),
])

df_afluencia = (spark.read
                .option("header", True)
                .option("dateFormat", "yyyy-MM-dd")
                .schema(schema_afluencia)
                .csv(ruta)
                .withColumn("fecha_ingesta", current_timestamp()))

df_afluencia.select(*spark.table(tabla).columns).write.mode("overwrite").insertInto(tabla)

print(f"{tabla}: {spark.table(tabla).count()} filas")

# COMMAND ----------

dbutils.notebook.exit("OK")