# Databricks notebook source
# MAGIC %md
# MAGIC # 2. Ingesta Bronze – Tiendas (JSON multilínea)
# MAGIC Fuente: `raw/maestros/tiendas.json`.

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("storageName", "adlsmallsdata")
dbutils.widgets.text("container", "raw")
dbutils.widgets.text("catalogo", "catalog_dev")
dbutils.widgets.text("esquema", "bronze")

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, to_date
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

storageName = dbutils.widgets.get("storageName")
container = dbutils.widgets.get("container")
catalogo = dbutils.widgets.get("catalogo")
esquema = dbutils.widgets.get("esquema")

ruta = f"abfss://{container}@{storageName}.dfs.core.windows.net/maestros/tiendas.json"
tabla = f"{catalogo}.{esquema}.tiendas"

# COMMAND ----------

schema_tiendas = StructType([
    StructField("id_tienda", IntegerType(), False),
    StructField("nombre_tienda", StringType(), True),
    StructField("rubro", StringType(), True),
    StructField("id_mall", IntegerType(), True),
    StructField("piso", IntegerType(), True),
    StructField("area_m2", IntegerType(), True),
    StructField("fecha_apertura", StringType(), True),
])

df_tiendas = (spark.read
              .schema(schema_tiendas)
              .option("multiLine", True)
              .json(ruta))

display(df_tiendas.limit(10))

# COMMAND ----------

df_final = (df_tiendas
            .withColumn("fecha_apertura", to_date(col("fecha_apertura"), "yyyy-MM-dd"))
            .withColumn("fecha_ingesta", current_timestamp()))

df_final.select(*spark.table(tabla).columns).write.mode("overwrite").insertInto(tabla)

print(f"{tabla}: {spark.table(tabla).count()} filas")

# COMMAND ----------

dbutils.notebook.exit("OK")
