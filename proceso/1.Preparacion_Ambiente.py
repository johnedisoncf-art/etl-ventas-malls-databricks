# Databricks notebook source
# MAGIC %md
# MAGIC # 1. Preparación de ambiente
# MAGIC Crea los objetos de Unity Catalog para la arquitectura medallion del proyecto **Ventas de Centros Comerciales**:
# MAGIC
# MAGIC 1. **External Locations** sobre ADLS Gen2, usando la *Storage Credential* creada con el **Access Connector (Managed Identity)**.
# MAGIC 2. **Catálogo** del ambiente (`catalog_dev` / `catalog_prod`) con *managed location* propia.
# MAGIC 3. **Esquemas** `bronze`, `silver`, `golden`.
# MAGIC 4. **Tablas Delta externas** de cada capa (DDL).
# MAGIC
# MAGIC > La capa raw es un contenedor de ADLS (`raw`). No se usa DBFS ni Volumes.

# COMMAND ----------

dbutils.widgets.removeAll()

dbutils.widgets.text("storageName", "adlsmallsdata")
dbutils.widgets.text("catalogo", "catalog_dev")
dbutils.widgets.text("credencial", "cred_mi_malls")

# COMMAND ----------

storageName = dbutils.widgets.get("storageName")
catalogo = dbutils.widgets.get("catalogo")
credencial = dbutils.widgets.get("credencial")

def ruta(contenedor: str, sub: str = "") -> str:
    """abfss://<contenedor>@<storage>.dfs.core.windows.net/<sub>"""
    return f"abfss://{contenedor}@{storageName}.dfs.core.windows.net/{sub}"

print("Storage   :", storageName)
print("Catálogo  :", catalogo)
print("Credencial:", credencial)

# COMMAND ----------

# MAGIC %md
# MAGIC ## External Locations (Managed Identity)
# MAGIC Una por contenedor. Son objetos del metastore: si ya existen (por ejemplo, creados desde dev) se reutilizan.

# COMMAND ----------

for contenedor in ["metastore", "raw", "bronze", "silver", "golden"]:
    spark.sql(f"""
        CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt-{contenedor}`
        URL '{ruta(contenedor)}'
        WITH (STORAGE CREDENTIAL `{credencial}`)
        COMMENT 'Contenedor {contenedor} - proyecto ventas malls'
    """)
display(spark.sql("SHOW EXTERNAL LOCATIONS"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Catálogo y esquemas

# COMMAND ----------

spark.sql(f"""
    CREATE CATALOG IF NOT EXISTS {catalogo}
    MANAGED LOCATION '{ruta("metastore", catalogo)}'
    COMMENT 'Arquitectura medallion - ventas de centros comerciales'
""")

for esquema in ["bronze", "silver", "golden"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalogo}.{esquema} COMMENT 'Capa {esquema}'")

display(spark.sql(f"SHOW SCHEMAS IN {catalogo}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tablas Bronze
# MAGIC Datos tal como llegan del raw + columnas de auditoría. Las ventas quedan como `string` (se tipan en silver).

# COMMAND ----------

def crear_tabla(capa: str, nombre: str, columnas: str, particion: str = ""):
    part = f"PARTITIONED BY ({particion})" if particion else ""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalogo}.{capa}.{nombre} (
            {columnas}
        )
        USING DELTA
        {part}
        LOCATION '{ruta(capa, f"{catalogo}/{nombre}")}'
    """)
    print(f"OK {catalogo}.{capa}.{nombre}")

crear_tabla("bronze", "centros_comerciales", """
    id_mall INT, nombre_mall STRING, operador STRING, distrito STRING,
    area_m2 INT, anio_apertura INT, fecha_ingesta TIMESTAMP""")

crear_tabla("bronze", "tiendas", """
    id_tienda INT, nombre_tienda STRING, rubro STRING, id_mall INT, piso INT,
    area_m2 INT, fecha_apertura DATE, fecha_ingesta TIMESTAMP""")

crear_tabla("bronze", "ventas", """
    id_venta STRING, fecha_venta STRING, id_tienda STRING, id_mall STRING,
    categoria STRING, cantidad STRING, monto_soles STRING, medio_pago STRING,
    archivo_origen STRING, fecha_ingesta TIMESTAMP""")

crear_tabla("bronze", "afluencia", """
    fecha DATE, id_mall INT, visitantes INT, fecha_ingesta TIMESTAMP""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tablas Silver
# MAGIC Datos limpios, tipados y enriquecidos (modelo dimensional).

# COMMAND ----------

crear_tabla("silver", "dim_mall", """
    id_mall INT, nombre_mall STRING, operador STRING, distrito STRING, area_m2 INT,
    anio_apertura INT, antiguedad_anios INT, tamano_mall STRING, fecha_proceso TIMESTAMP""")

crear_tabla("silver", "dim_tienda", """
    id_tienda INT, nombre_tienda STRING, rubro STRING, id_mall INT, nombre_mall STRING,
    piso INT, area_m2 INT, fecha_apertura DATE, formato_tienda STRING, fecha_proceso TIMESTAMP""")

crear_tabla("silver", "fact_ventas", """
    id_venta BIGINT, fecha_venta DATE, anio INT, mes INT, dia_semana STRING, es_fin_de_semana BOOLEAN,
    id_tienda INT, id_mall INT, rubro STRING, cantidad INT, monto_soles DOUBLE,
    precio_unitario DOUBLE, rango_ticket STRING, medio_pago STRING, fecha_proceso TIMESTAMP""",
    particion="anio, mes")

crear_tabla("silver", "afluencia_diaria", """
    fecha DATE, id_mall INT, visitantes INT, es_fin_de_semana BOOLEAN, fecha_proceso TIMESTAMP""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tablas Golden
# MAGIC Agregados de negocio listos para el dashboard.

# COMMAND ----------

crear_tabla("golden", "ventas_mensuales_mall", """
    id_mall INT, nombre_mall STRING, operador STRING, anio INT, mes INT,
    total_ventas DOUBLE, num_tickets BIGINT, unidades BIGINT, ticket_promedio DOUBLE,
    crecimiento_vs_mes_anterior_pct DOUBLE, fecha_carga TIMESTAMP""")

crear_tabla("golden", "ranking_tiendas", """
    id_mall INT, nombre_mall STRING, id_tienda INT, nombre_tienda STRING, rubro STRING,
    total_ventas DOUBLE, num_tickets BIGINT, participacion_mall_pct DOUBLE,
    ranking_en_mall INT, fecha_carga TIMESTAMP""")

crear_tabla("golden", "conversion_diaria_mall", """
    fecha DATE, id_mall INT, nombre_mall STRING, visitantes INT, num_tickets BIGINT,
    total_ventas DOUBLE, tasa_conversion_pct DOUBLE, venta_por_visitante DOUBLE,
    es_fin_de_semana BOOLEAN, fecha_carga TIMESTAMP""")

crear_tabla("golden", "ventas_rubro_medio_pago", """
    rubro STRING, efectivo DOUBLE, plin DOUBLE, tarjeta_credito DOUBLE,
    tarjeta_debito DOUBLE, yape DOUBLE, total DOUBLE, fecha_carga TIMESTAMP""")

# COMMAND ----------

dbutils.notebook.exit("OK - ambiente preparado")
