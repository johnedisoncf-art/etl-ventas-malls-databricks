# Databricks notebook source
# MAGIC %md
# MAGIC # 4. Load – Silver ➜ Golden
# MAGIC Tablas de negocio para el dashboard:
# MAGIC | Tabla | Pregunta que responde | Técnica |
# MAGIC |---|---|---|
# MAGIC | `ventas_mensuales_mall` | ¿Cuánto vende cada mall por mes y cómo crece? | groupBy + Window `lag` |
# MAGIC | `ranking_tiendas` | ¿Qué tiendas lideran en cada mall? | Window `rank` + participación |
# MAGIC | `conversion_diaria_mall` | ¿Qué % de visitantes compra? | join silver ventas + afluencia |
# MAGIC | `ventas_rubro_medio_pago` | ¿Cómo pagan en cada rubro? | `pivot` |

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("catalogo", "catalog_dev")
dbutils.widgets.text("esquema_source", "silver")
dbutils.widgets.text("esquema_sink", "golden")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import col, current_timestamp, broadcast
from pyspark.sql.window import Window

catalogo = dbutils.widgets.get("catalogo")
src = f"{catalogo}.{dbutils.widgets.get('esquema_source')}"
snk = f"{catalogo}.{dbutils.widgets.get('esquema_sink')}"

def escribir_overwrite(df, tabla):
    df.select(*spark.table(tabla).columns).write.mode("overwrite").insertInto(tabla)
    print(f"{tabla}: {spark.table(tabla).count()} filas")

fact = spark.table(f"{src}.fact_ventas").cache()   # se reutiliza en varias agregaciones
dim_mall = spark.table(f"{src}.dim_mall")
dim_tienda = spark.table(f"{src}.dim_tienda")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ventas_mensuales_mall

# COMMAND ----------

w_mes = Window.partitionBy("id_mall").orderBy("anio", "mes")

df_mensual = (fact.groupBy("id_mall", "anio", "mes")
              .agg(F.round(F.sum("monto_soles"), 2).alias("total_ventas"),
                   F.count("id_venta").alias("num_tickets"),
                   F.sum("cantidad").cast("bigint").alias("unidades"))
              .withColumn("ticket_promedio", F.round(col("total_ventas") / col("num_tickets"), 2))
              .withColumn("venta_mes_anterior", F.lag("total_ventas").over(w_mes))
              .withColumn("crecimiento_vs_mes_anterior_pct",
                          F.round((col("total_ventas") / col("venta_mes_anterior") - 1) * 100, 2))
              .join(broadcast(dim_mall.select("id_mall", "nombre_mall", "operador")), "id_mall")
              .withColumn("fecha_carga", current_timestamp()))

escribir_overwrite(df_mensual, f"{snk}.ventas_mensuales_mall")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ranking_tiendas

# COMMAND ----------

w_rank = Window.partitionBy("id_mall").orderBy(col("total_ventas").desc())
w_mall = Window.partitionBy("id_mall")

df_ranking = (fact.groupBy("id_mall", "id_tienda")
              .agg(F.round(F.sum("monto_soles"), 2).alias("total_ventas"),
                   F.count("id_venta").alias("num_tickets"))
              .withColumn("participacion_mall_pct",
                          F.round(col("total_ventas") / F.sum("total_ventas").over(w_mall) * 100, 2))
              .withColumn("ranking_en_mall", F.rank().over(w_rank))
              .join(dim_tienda.select("id_tienda", "nombre_tienda", "rubro", "nombre_mall"), "id_tienda")
              .withColumn("fecha_carga", current_timestamp()))

escribir_overwrite(df_ranking, f"{snk}.ranking_tiendas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## conversion_diaria_mall

# COMMAND ----------

ventas_dia = (fact.groupBy(col("fecha_venta").alias("fecha"), "id_mall")
              .agg(F.count("id_venta").alias("num_tickets"),
                   F.round(F.sum("monto_soles"), 2).alias("total_ventas")))

df_conv = (spark.table(f"{src}.afluencia_diaria").alias("a")
           .join(ventas_dia.alias("v"), ["fecha", "id_mall"], "left")
           .fillna({"num_tickets": 0, "total_ventas": 0.0})
           .withColumn("tasa_conversion_pct", F.round(col("num_tickets") / col("visitantes") * 100, 3))
           .withColumn("venta_por_visitante", F.round(col("total_ventas") / col("visitantes"), 2))
           .join(broadcast(dim_mall.select("id_mall", "nombre_mall")), "id_mall")
           .withColumn("fecha_carga", current_timestamp()))

escribir_overwrite(df_conv, f"{snk}.conversion_diaria_mall")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ventas_rubro_medio_pago (pivot)

# COMMAND ----------

medios = {"Efectivo": "efectivo", "Plin": "plin", "Tarjeta Credito": "tarjeta_credito",
          "Tarjeta Debito": "tarjeta_debito", "Yape": "yape"}

df_pivot = (fact.groupBy("rubro")
            .pivot("medio_pago", list(medios.keys()))
            .agg(F.round(F.sum("monto_soles"), 2))
            .fillna(0))
for original, nuevo in medios.items():
    df_pivot = df_pivot.withColumnRenamed(original, nuevo)

df_pivot = (df_pivot
            .withColumn("total", F.round(sum(col(c) for c in medios.values()), 2))
            .withColumn("fecha_carga", current_timestamp()))

escribir_overwrite(df_pivot, f"{snk}.ventas_rubro_medio_pago")

# COMMAND ----------

fact.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mantenimiento Delta: OPTIMIZE + ZORDER

# COMMAND ----------

spark.sql(f"OPTIMIZE {src}.fact_ventas ZORDER BY (id_mall, id_tienda)")
spark.sql(f"OPTIMIZE {snk}.conversion_diaria_mall ZORDER BY (fecha)")

# COMMAND ----------

dbutils.notebook.exit("OK")
