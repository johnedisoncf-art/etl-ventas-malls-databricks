# Databricks notebook source
# MAGIC %md
# MAGIC # Seguridad – revocar permisos y auditar grants
# MAGIC Complemento de `proceso/5.Grants_Medallion`.

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("catalogo", "catalog_prod")
dbutils.widgets.text("grupo", "analistas_malls")

catalogo = dbutils.widgets.get("catalogo")
grupo = dbutils.widgets.get("grupo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auditoría

# COMMAND ----------

display(spark.sql(f"SHOW GRANTS ON CATALOG {catalogo}"))

# COMMAND ----------

for esquema in ["bronze", "silver", "golden"]:
    print(f"--- {catalogo}.{esquema}")
    display(spark.sql(f"SHOW GRANTS ON SCHEMA {catalogo}.{esquema}"))

# COMMAND ----------

display(spark.sql("SHOW GRANTS ON EXTERNAL LOCATION `exlt-raw`"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Revocar

# COMMAND ----------

spark.sql(f"REVOKE USE SCHEMA, SELECT ON SCHEMA {catalogo}.golden FROM `{grupo}`")
spark.sql(f"REVOKE USE CATALOG ON CATALOG {catalogo} FROM `{grupo}`")
display(spark.sql(f"SHOW GRANTS `{grupo}` ON CATALOG {catalogo}"))
