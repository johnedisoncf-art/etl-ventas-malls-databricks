# Databricks notebook source
# MAGIC %md
# MAGIC # 5. Grants – Gobierno con Unity Catalog
# MAGIC * `grupoIngenieros`: lectura/escritura en las 3 capas.
# MAGIC * `grupoAnalistas`: solo lectura sobre **golden** (lo que consume el dashboard).
# MAGIC
# MAGIC Los grupos deben existir a nivel de cuenta (Account Console ➜ User management ➜ Groups) y estar asignados al workspace.

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("catalogo", "catalog_dev")
dbutils.widgets.text("grupoIngenieros", "data_engineers")
dbutils.widgets.text("grupoAnalistas", "analistas_malls")

# COMMAND ----------

catalogo = dbutils.widgets.get("catalogo")
ing = dbutils.widgets.get("grupoIngenieros")
ana = dbutils.widgets.get("grupoAnalistas")

grants = [
    # Ingenieros de datos
    f"GRANT USE CATALOG ON CATALOG {catalogo} TO `{ing}`",
    *[f"GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA {catalogo}.{e} TO `{ing}`"
      for e in ["bronze", "silver", "golden"]],
    # Analistas / BI
    f"GRANT USE CATALOG ON CATALOG {catalogo} TO `{ana}`",
    f"GRANT USE SCHEMA, SELECT ON SCHEMA {catalogo}.golden TO `{ana}`",
]

errores = []
for g in grants:
    try:
        spark.sql(g)
        print("OK   ", g)
    except Exception as e:
        errores.append(g)
        print("ERROR", g, "->", str(e).splitlines()[0])

# COMMAND ----------

display(spark.sql(f"SHOW GRANTS ON SCHEMA {catalogo}.golden"))

# COMMAND ----------

# Si algún grant falla (p.ej. el grupo no existe) la tarea falla y el workflow lo muestra.
if errores:
    raise Exception(f"{len(errores)} grants fallaron. ¿Existen los grupos '{ing}' y '{ana}' en la cuenta?")

dbutils.notebook.exit("OK")
