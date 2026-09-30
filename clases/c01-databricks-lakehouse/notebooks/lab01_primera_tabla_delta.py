# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 1 — Primer activo gobernado en el Lakehouse
# MAGIC
# MAGIC Objetivo: dejar `DemandaPerdidas.xlsx` como tabla Delta en Unity Catalog, con metadatos de ingesta y con historial.
# MAGIC
# MAGIC Antes de ejecutar: crea el esquema y el volumen (paso 3 de `lab.md`) y sube el Excel al volumen.

# COMMAND ----------

# MAGIC %pip install -q openpyxl

# COMMAND ----------

dbutils.widgets.text("usuario", "juancuartas")
dbutils.widgets.text("archivo", "DemandaPerdidas.xlsx")

usuario = dbutils.widgets.get("usuario").strip().lower()
archivo = dbutils.widgets.get("archivo")
assert usuario, "Escribe tu usuario en el widget 'usuario' (sin puntos ni tildes)."

catalog = "workspace"
schema = f"{catalog}.c01_{usuario}"
vol = f"/Volumes/{catalog}/c01_{usuario}/raw"
tabla = f"{schema}.demanda_raw"

print(f"Esquema: {schema}\nVolumen: {vol}\nTabla:   {tabla}")

# COMMAND ----------

dbutils.widgets.text("usuario", "juancuartas")
dbutils.widgets.text("archivo", "DemandaPerdidas.xlsx")

usuario = dbutils.widgets.get("usuario").strip().lower()
archivo = dbutils.widgets.get("archivo")
assert usuario, "juancuartas"

catalog = "workspace"
schema = f"{catalog}.c01_{usuario}"
vol = f"/Volumes/{catalog}/c01_{usuario}/raw"
tabla = f"{schema}.demanda_raw"

print(f"Esquema: {schema}\nVolumen: {vol}\nTabla:   {tabla}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: del archivo a Delta
# MAGIC
# MAGIC Regla de Bronze: el dato entra **tal cual**, más metadatos de ingesta. Nada de limpiar aquí.

# COMMAND ----------

import pandas as pd
from pyspark.sql import functions as F

pdf = pd.read_excel(f"{vol}/{archivo}")
print(pdf.shape)
pdf.head()

# COMMAND ----------

import pandas as pd
from pyspark.sql import functions as F

pdf = pd.read_excel(f"{vol}/{archivo}")
print(pdf.shape)
pdf.head()

# COMMAND ----------

df = (
    spark.createDataFrame(pdf)
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.lit(archivo))
)

df.write.mode("overwrite").saveAsTable(tabla)
print(f"Escrita {tabla}")

# COMMAND ----------

df = (
    spark.createDataFrame(pdf)
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.lit(archivo))
)

df.write.mode("overwrite").saveAsTable(tabla)
print(f"Escrita {tabla}")

# COMMAND ----------

spark.sql(f"SELECT count(*) AS filas FROM {tabla}").display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificación

# COMMAND ----------

spark.sql(f"SELECT count(*) AS filas FROM {tabla}").display()

# COMMAND ----------

spark.sql(f"""
SELECT CodigoVariable, count(*) AS filas, min(Fecha) AS desde, max(Fecha) AS hasta
FROM {tabla}
GROUP BY CodigoVariable
""").display()


# COMMAND ----------

spark.sql(f"""
SELECT CodigoVariable, count(*) AS filas, min(Fecha) AS desde, max(Fecha) AS hasta
FROM {tabla}
GROUP BY CodigoVariable
""").display()

# COMMAND ----------

spark.sql(f"""
SELECT count(*) AS series_activas
FROM (
  SELECT DISTINCT CodigoSICAgente, MercadoComercializacion, TipoMercado, ClasificacionIndustrial
  FROM {tabla}
)
""").display()

# COMMAND ----------

spark.sql(f"""
SELECT count(*) AS series_activas
FROM (
  SELECT DISTINCT CodigoSICAgente, MercadoComercializacion, TipoMercado, ClasificacionIndustrial
  FROM {tabla}
)
""").display()

# COMMAND ----------

# Lo mismo en PySpark
llave = ["CodigoSICAgente", "MercadoComercializacion", "TipoMercado", "ClasificacionIndustrial"]
spark.table(tabla).select(*llave).distinct().count()

# COMMAND ----------

# Lo mismo en PySpark
llave = ["CodigoSICAgente", "MercadoComercializacion", "TipoMercado", "ClasificacionIndustrial"]
spark.table(tabla).select(*llave).distinct().count()

# COMMAND ----------

# Lo mismo en PySpark
llave = ["CodigoSICAgente", "MercadoComercializacion", "TipoMercado", "ClasificacionIndustrial"]
spark.table(tabla).select(*llave).distinct().count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Historial y time travel
# MAGIC
# MAGIC Vuelve a ejecutar la celda de escritura (arriba) y luego corre estas dos.

# COMMAND ----------

spark.sql(f"DESCRIBE HISTORY {tabla}").select("version", "timestamp", "operation", "operationMetrics").display()

# COMMAND ----------

spark.sql(f"DESCRIBE HISTORY {tabla}").select("version", "timestamp", "operation", "operationMetrics").display()

# COMMAND ----------

spark.sql(f"SELECT count(*) AS filas_version_0 FROM {tabla} VERSION AS OF 0").display()

# COMMAND ----------

spark.sql(f"SELECT count(*) AS filas_version_0 FROM {tabla} VERSION AS OF 0").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificación automática
# MAGIC
# MAGIC Comprueba los criterios de aceptación del lab. Corre esta celda cuando creas que terminaste.

# COMMAND ----------

def verificar():
    checks = []
    cols = set(spark.table(tabla).columns)
    checks.append(("Tabla con 145.408 filas", spark.table(tabla).count() == 145_408))
    checks.append(("Columnas _ingested_at y _source_file", {"_ingested_at", "_source_file"} <= cols))
    checks.append(("355 series activas", spark.table(tabla).select(*llave).distinct().count() == 355))
    versiones = spark.sql(f"DESCRIBE HISTORY {tabla}").count()
    checks.append(("Al menos 2 versiones en el historial", versiones >= 2))
    for nombre, ok in checks:
        print(("OK   " if ok else "FALTA") + "  " + nombre)
    print("\nListo: commit y push." if all(ok for _, ok in checks) else "\nRevisa los puntos marcados FALTA.")


verificar()

# COMMAND ----------


def verificar():
    checks = []
    cols = set(spark.table(tabla).columns)
    checks.append(("Tabla con 145.408 filas", spark.table(tabla).count() == 145_408))
    checks.append(("Columnas _ingested_at y _source_file", {"_ingested_at", "_source_file"} <= cols))
    checks.append(("355 series activas", spark.table(tabla).select(*llave).distinct().count() == 355))
    versiones = spark.sql(f"DESCRIBE HISTORY {tabla}").count()
    checks.append(("Al menos 2 versiones en el historial", versiones >= 2))
    for nombre, ok in checks:
        print(("OK   " if ok else "FALTA") + "  " + nombre)
    print("\nListo: commit y push." if all(ok for _, ok in checks) else "\nRevisa los puntos marcados FALTA.")


verificar()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extensión opcional
# MAGIC
# MAGIC ¿Cuántas series tienen datos los 206 días? ¿Cuáles no y cuántos días les faltan?

# COMMAND ----------

spark.sql(f"""
SELECT min(Fecha) AS fecha_inicial,
       max(Fecha) AS fecha_final,
       datediff(max(Fecha), min(Fecha)) AS dias_entre_fechas
FROM {tabla}
""").display()

# COMMAND ----------

dias_esperados = 206

print(f"=== Series con los {dias_esperados} días completos ===")
spark.sql(f"""
SELECT CodigoSICAgente, MercadoComercializacion, TipoMercado, ClasificacionIndustrial,
       count(DISTINCT Fecha) AS dias_con_datos
FROM {tabla}
GROUP BY CodigoSICAgente, MercadoComercializacion, TipoMercado, ClasificacionIndustrial
HAVING count(DISTINCT Fecha) = {dias_esperados}
""").display()

print(f"\n=== Series con datos FALTANTES ===")
spark.sql(f"""
SELECT CodigoSICAgente, MercadoComercializacion, TipoMercado, ClasificacionIndustrial,
       count(DISTINCT Fecha) AS dias_con_datos,
       {dias_esperados} - count(DISTINCT Fecha) AS dias_faltantes
FROM {tabla}
GROUP BY CodigoSICAgente, MercadoComercializacion, TipoMercado, ClasificacionIndustrial
HAVING count(DISTINCT Fecha) < {dias_esperados}
ORDER BY dias_faltantes DESC
""").display()

# Resumen rápido
resumen = spark.sql(f"""
SELECT CASE WHEN dias_con_datos = {dias_esperados} THEN 'completas' ELSE 'incompletas' END AS estado,
       count(*) AS series
FROM (
  SELECT CodigoSICAgente, MercadoComercializacion, TipoMercado, ClasificacionIndustrial,
         count(DISTINCT Fecha) AS dias_con_datos
  FROM {tabla}
  GROUP BY CodigoSICAgente, MercadoComercializacion, TipoMercado, ClasificacionIndustrial
)
GROUP BY CASE WHEN dias_con_datos = {dias_esperados} THEN 'completas' ELSE 'incompletas' END
""")
resumen.display()