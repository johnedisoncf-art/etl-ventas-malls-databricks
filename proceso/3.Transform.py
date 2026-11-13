# Databricks notebook source
# MAGIC %md
# MAGIC # 3. Transform – Bronze ➜ Silver
# MAGIC * Limpieza: nulos, negativos, duplicados, espacios y mayúsculas inconsistentes, fechas en dos formatos.
# MAGIC * Tipado de columnas.
# MAGIC * Enriquecimiento: UDF, `when/otherwise`, joins con *broadcast*, funciones de fecha.
# MAGIC * Deduplicación con **Window + row_number**.
# MAGIC * Carga incremental del hecho con **MERGE (upsert) de Delta Lake**.

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("catalogo", "catalog_dev")
dbutils.widgets.text("esquema_source", "bronze")
dbutils.widgets.text("esquema_sink", "silver")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, lit, trim, initcap, broadcast, current_timestamp
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
from delta.tables import DeltaTable

catalogo = dbutils.widgets.get("catalogo")
src = f"{catalogo}.{dbutils.widgets.get('esquema_source')}"
snk = f"{catalogo}.{dbutils.widgets.get('esquema_sink')}"

def escribir_overwrite(df, tabla):
    """insertInto es posicional: se alinean las columnas al orden de la tabla destino."""
    df.select(*spark.table(tabla).columns).write.mode("overwrite").insertInto(tabla)
    print(f"{tabla}: {spark.table(tabla).count()} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_mall

# COMMAND ----------

df_mall = (spark.table(f"{src}.centros_comerciales")
           .dropna(subset=["id_mall"])
           .dropDuplicates(["id_mall"])
           .withColumn("nombre_mall", trim(col("nombre_mall")))
           .withColumn("antiguedad_anios", F.year(F.current_date()) - col("anio_apertura"))
           .withColumn("tamano_mall",
                       when(col("area_m2") >= 100000, "Super regional")
                       .when(col("area_m2") >= 60000, "Regional")
                       .otherwise("Comunitario"))
           .withColumn("fecha_proceso", current_timestamp()))

escribir_overwrite(df_mall, f"{snk}.dim_mall")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_tienda (join broadcast con dim_mall)

# COMMAND ----------

df_tienda = (spark.table(f"{src}.tiendas")
             .dropna(subset=["id_tienda", "id_mall"])
             .dropDuplicates(["id_tienda"])
             .withColumn("nombre_tienda", trim(col("nombre_tienda")))
             .withColumn("rubro", trim(col("rubro"))))

df_tienda = (df_tienda.alias("t")
             .join(broadcast(df_mall.select("id_mall", "nombre_mall")).alias("m"), "id_mall", "inner")
             .withColumn("formato_tienda",
                         when(col("area_m2") >= 1500, "Ancla")
                         .when(col("area_m2") >= 250, "Mediana")
                         .otherwise("Pequeña"))
             .withColumn("fecha_proceso", current_timestamp()))

escribir_overwrite(df_tienda, f"{snk}.dim_tienda")

# COMMAND ----------

# MAGIC %md
# MAGIC ## fact_ventas

# COMMAND ----------

# UDF (visto en clase): clasifica el ticket por monto
def rango_ticket(monto):
    if monto is None:
        return None
    if monto < 50:
        return "Bajo (<S/50)"
    if monto < 200:
        return "Medio (S/50-199)"
    if monto < 1000:
        return "Alto (S/200-999)"
    return "Premium (>=S/1000)"

rango_ticket_udf = F.udf(rango_ticket, StringType())

# COMMAND ----------

df_bronze = spark.table(f"{src}.ventas")
total_bronze = df_bronze.count()

# 1) Tipado + normalización de texto + fecha en 2 formatos posibles
#    Databricks trabaja en modo ANSI: cast/to_date fallan ante un valor inválido.
#    try_cast / try_to_date devuelven NULL y la regla de calidad (paso 2) descarta la fila.
df_v = (df_bronze
        .withColumn("id_venta", F.expr("try_cast(id_venta AS BIGINT)"))
        .withColumn("id_tienda", F.expr("try_cast(id_tienda AS INT)"))
        .withColumn("id_mall", F.expr("try_cast(id_mall AS INT)"))
        .withColumn("cantidad", F.expr("try_cast(cantidad AS INT)"))
        .withColumn("monto_soles", F.round(F.expr("try_cast(monto_soles AS DOUBLE)"), 2))
        .withColumn("medio_pago", initcap(trim(col("medio_pago"))))
        .withColumn("categoria", trim(col("categoria")))
        .withColumn("fecha_venta", F.coalesce(F.expr("try_to_date(fecha_venta, 'yyyy-MM-dd')"),
                                              F.expr("try_to_date(fecha_venta, 'dd/MM/yyyy')"))))

# 2) Reglas de calidad
df_v = df_v.filter(col("id_venta").isNotNull()
                   & col("fecha_venta").isNotNull()
                   & col("monto_soles").isNotNull() & (col("monto_soles") > 0)
                   & col("cantidad").isNotNull() & (col("cantidad") > 0))

# 3) Deduplicación: se queda la última versión ingerida de cada id_venta
w = Window.partitionBy("id_venta").orderBy(col("fecha_ingesta").desc())
df_v = df_v.withColumn("rn", F.row_number().over(w)).filter(col("rn") == 1).drop("rn")

# COMMAND ----------

# 4) Enriquecimiento: rubro oficial desde dim_tienda (broadcast), atributos de fecha, UDF
df_fact = (df_v.alias("v")
           .join(broadcast(df_tienda.select("id_tienda", col("rubro").alias("rubro_tienda"))),
                 "id_tienda", "inner")
           .withColumn("rubro", F.coalesce(col("rubro_tienda"), initcap(col("categoria"))))
           .withColumn("anio", F.year("fecha_venta"))
           .withColumn("mes", F.month("fecha_venta"))
           .withColumn("dia_semana", F.date_format("fecha_venta", "EEEE"))
           .withColumn("es_fin_de_semana", F.dayofweek("fecha_venta").isin(1, 7))
           .withColumn("precio_unitario", F.round(col("monto_soles") / col("cantidad"), 2))
           .withColumn("rango_ticket", rango_ticket_udf(col("monto_soles")))
           .withColumn("fecha_proceso", current_timestamp())
           .select(*spark.table(f"{snk}.fact_ventas").columns))

print(f"Bronze: {total_bronze} | Silver válidas: {df_fact.count()} | Descartadas: {total_bronze - df_fact.count()}")

# COMMAND ----------

# 5) MERGE (upsert) en Delta: idempotente, se puede re-ejecutar sin duplicar
destino = DeltaTable.forName(spark, f"{snk}.fact_ventas")

(destino.alias("d")
 .merge(df_fact.alias("s"), "d.id_venta = s.id_venta")
 .whenMatchedUpdateAll()
 .whenNotMatchedInsertAll()
 .execute())

print(f"{snk}.fact_ventas: {spark.table(f'{snk}.fact_ventas').count()} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## afluencia_diaria

# COMMAND ----------

df_af = (spark.table(f"{src}.afluencia")
         .dropna(subset=["fecha", "id_mall"])
         .filter(col("visitantes") > 0)
         .dropDuplicates(["fecha", "id_mall"])
         .withColumn("es_fin_de_semana", F.dayofweek("fecha").isin(1, 7))
         .withColumn("fecha_proceso", current_timestamp()))

escribir_overwrite(df_af, f"{snk}.afluencia_diaria")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Historial Delta (time travel)

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {snk}.fact_ventas").select("version", "timestamp", "operation", "operationMetrics").limit(5))

# COMMAND ----------

dbutils.notebook.exit("OK")
