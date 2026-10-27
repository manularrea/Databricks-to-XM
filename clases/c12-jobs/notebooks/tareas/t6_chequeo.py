# Databricks notebook source
# MAGIC %md
# MAGIC # Tarea 6 · chequeo: ¿lo publicado está al día y completo?
# MAGIC Parámetros del job: `usuario`, `fecha_proceso`. Corre siempre (`run_if = ALL_DONE`), aunque lo anterior haya
# MAGIC fallado o se haya excluido. Convierte la falla silenciosa (todo en verde sobre datos de ayer) en una falla visible que
# MAGIC dispara la alerta del job. Las reglas están en `src/xm_demanda/orquestacion/chequeo.py`.

# COMMAND ----------

dbutils.widgets.text("usuario", "")
dbutils.widgets.text("fecha_proceso", "2026-07-26")
usuario = dbutils.widgets.get("usuario").strip().lower()
fecha_proceso = dbutils.widgets.get("fecha_proceso").strip()
esquema = f"workspace.c01_{usuario}"
T_FEATURES, T_PRON = f"{esquema}.features_demanda_diaria", f"{esquema}.pronostico_demanda"

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

from datetime import date

from xm_demanda.orquestacion.chequeo import problemas_de_frescura
from xm_demanda.utils.config import load_config

H = load_config("dev")["modelo"]["horizon_days"]
ultima_oro = spark.sql(f"SELECT max(fecha) FROM {T_FEATURES}").first()[0]
fechas = [r[0] for r in spark.sql(f"SELECT DISTINCT fecha_pronostico FROM {T_PRON} WHERE alias = 'champion'").collect()]
filas_nuevas = dbutils.jobs.taskValues.get(taskKey="bronce", key="filas_nuevas", default=-1, debugValue=-1)

problemas = problemas_de_frescura(ultima_oro, date.fromisoformat(fecha_proceso), fechas, H)
hasta = max(fechas) if fechas else "sin pronóstico"
print(f"chequeo · proceso {fecha_proceso} · oro hasta {ultima_oro} · pronóstico hasta {hasta} · filas nuevas en bronce: {filas_nuevas}")
if problemas:
    raise AssertionError("Pronóstico NO publicable:\n- " + "\n- ".join(problemas)
                         + "\nRevisa la tarea bronce (¿llegó el archivo del día?) antes de hacer Repair run.")
print("OK: datos al día y pronóstico completo para los próximos", H, "días")
