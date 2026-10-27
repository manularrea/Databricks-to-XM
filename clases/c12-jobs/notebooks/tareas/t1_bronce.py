# Databricks notebook source
# MAGIC %md
# MAGIC # Tarea 1 · bronce: ingesta incremental con Auto Loader
# MAGIC Parámetros del job: `usuario`, `forzar`. Lee los archivos nuevos de landing (`availableNow`), refresca la vista
# MAGIC vigente y publica dos *task values* para el resto del job: `filas_nuevas` y `procesar` (`si`/`no`), que usa la
# MAGIC tarea condicional `hay_datos_nuevos`.

# COMMAND ----------

dbutils.widgets.text("usuario", "")
dbutils.widgets.text("forzar", "si")
usuario = dbutils.widgets.get("usuario").strip().lower()
forzar = dbutils.widgets.get("forzar").strip().lower() == "si"
assert usuario, "Falta el parámetro usuario"
esquema = f"workspace.c01_{usuario}"
volumen = f"/Volumes/workspace/c01_{usuario}/raw"
landing, checkpoint = f"{volumen}/landing", f"{volumen}/_checkpoints/bronze"
tabla = f"{esquema}.demanda_raw_bronze"

# COMMAND ----------

import os
import sys


def _raiz_repo():
    d = os.getcwd()
    for _ in range(8):
        if os.path.exists(os.path.join(d, "src", "xm_demanda")):
            return d
        d = os.path.dirname(d)
    raise FileNotFoundError("No encuentro src/xm_demanda: el job debe apuntar a los notebooks del Git folder.")


raiz = _raiz_repo()
sys.path.insert(0, os.path.join(raiz, "src"))
sys.dont_write_bytecode = True

import xm_demanda.ingest.bronze as bronze

antes = spark.table(tabla).count() if spark.catalog.tableExists(tabla) else 0
bronze.ingestar_bronce(spark, landing, tabla, checkpoint)
despues = spark.table(tabla).count()
nuevas = despues - antes

spark.sql(f"""
CREATE OR REPLACE VIEW {esquema}.demanda_raw_vigente
COMMENT 'Última publicación por serie-día-variable sobre demanda_raw_bronze'
AS
SELECT * EXCEPT (rn) FROM (
  SELECT *,
    row_number() OVER (
      PARTITION BY Fecha, CodigoVariable, CodigoSICAgente, MercadoComercializacion, TipoMercado, ClasificacionIndustrial
      ORDER BY _publication_date DESC, _ingested_at DESC
    ) AS rn
  FROM {tabla})
WHERE rn = 1
""")

procesar = "si" if nuevas > 0 or forzar else "no"
dbutils.jobs.taskValues.set(key="filas_nuevas", value=nuevas)
dbutils.jobs.taskValues.set(key="procesar", value=procesar)
print(f"bronce: {antes:,} → {despues:,} filas ({nuevas:,} nuevas) · forzar={forzar} · procesar={procesar}")
