# Databricks notebook source
# MAGIC %md
# MAGIC # Clase 12 — Orquestación con Lakeflow Jobs: el pipeline diario que se cuida solo
# MAGIC
# MAGIC Hasta hoy cada paso lo corrimos a mano, en un notebook por clase. En producción nadie abre un notebook a las 6 de la
# MAGIC mañana: lo hace un **job**. Hoy encadenamos las clases 4 a 11 en un solo job, como código, y lo ponemos a prueba con la
# MAGIC falla más peligrosa que existe: la que no falla.
# MAGIC
# MAGIC ```
# MAGIC   [xm-demanda] diario · <usuario>            parámetros: usuario · fecha_proceso · forzar · umbral_cuarentena
# MAGIC
# MAGIC   bronce ──► hay_datos_nuevos ──(true)──► calidad ──► plata ──► oro ──► inferencia ──► chequeo
# MAGIC   clase 4    if/else sobre un            clase 5     clase 6   clase 7  clase 11       run_if ALL_DONE
# MAGIC              task value de bronce                                       2 reintentos   corre siempre
# MAGIC                    └──(false)── la rama se excluye ────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC Tres corridas: (1) un día normal; (2) **el archivo del día no llegó**: todo termina "bien" y solo el chequeo lo
# MAGIC detecta; (3) el archivo llega tarde y se repara la corrida fallida con **Repair run**, sin volver a correr lo que ya estaba
# MAGIC bien. Un `TODO`: `politica()` en `src/xm_demanda/orquestacion/job.py` (sección 3). Dos preguntas por escrito.
# MAGIC Requiere los labs 4 a 11 en tu esquema (bronce, vigente, validada, `dim_ciiu`, plata, oro, modelo con alias y pronósticos).

# COMMAND ----------

dbutils.widgets.text("usuario", "")
usuario = dbutils.widgets.get("usuario").strip().lower()
assert usuario, "Escribe tu usuario (el sufijo de tu esquema c01_<usuario>)."
esquema = f"workspace.c01_{usuario}"

# COMMAND ----------

import importlib
import os
import sys
import time

import pandas as pd
from databricks.sdk import WorkspaceClient


def _raiz_repo():
    d = os.getcwd()
    for _ in range(8):
        if os.path.exists(os.path.join(d, "src", "xm_demanda")):
            return d
        d = os.path.dirname(d)
    raise FileNotFoundError("No encuentro src/xm_demanda: ejecuta este notebook desde tu Git folder.")


raiz = _raiz_repo()
sys.path.insert(0, os.path.join(raiz, "src"))
sys.dont_write_bytecode = True
import xm_demanda.orquestacion.job as jobdef

importlib.reload(jobdef)

w = WorkspaceClient()
email = w.current_user.me().user_name
ruta_tareas = os.path.join(os.getcwd(), "tareas").replace("/Workspace", "", 1)
print("Tareas del job en:", ruta_tareas, "| alertas a:", email)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Orquestar no es programar a una hora: es decidir qué pasa cuando algo sale mal
# MAGIC
# MAGIC ![Anatomía de un job](./../img/job_anatomia.png)
# MAGIC
# MAGIC | Pieza | Qué es | En nuestro job |
# MAGIC |---|---|---|
# MAGIC | **Job** | la unidad que se programa, se versiona y tiene dueño | `[xm-demanda] diario · <usuario>` |
# MAGIC | **Tarea** | un paso: notebook, script, pipeline declarativo, SQL, dbt, otro job, If/else, For each | 6 notebooks + 1 condición |
# MAGIC | **Dependencia** | el orden, como un grafo (DAG), no una lista | `depends_on` |
# MAGIC | **Parámetros del job** | entradas que llegan a todas las tareas como widgets | `usuario`, `fecha_proceso`, `forzar`… |
# MAGIC | **Task values** | salidas pequeñas de una tarea que leen las siguientes | `bronce` publica `filas_nuevas` y `procesar` |
# MAGIC | **If/else** | rama según un valor | si no hay datos nuevos, no se recalcula nada |
# MAGIC | **Run if** | cuándo corre una tarea según cómo terminaron las anteriores | `chequeo`: `ALL_DONE` (siempre) |
# MAGIC | **Reintentos y timeout** | qué hacer ante una falla transitoria o un proceso colgado | `inferencia`: 2 reintentos |
# MAGIC | **Disparadores** | calendario, llegada de archivo, actualización de tabla, continuo | calendario diario 6:00, pausado |
# MAGIC | **Notificaciones** | correo, Slack, Teams, webhook ante falla o demora | correo al fallar |
# MAGIC | **Repair run** | volver a correr solo lo que falló y lo que depende de ello | corrida 3 |
# MAGIC
# MAGIC Las tareas son **notebooks delgados** en `clases/c12-jobs/notebooks/tareas/`: cada uno lee sus parámetros y llama a las
# MAGIC funciones de `src/xm_demanda` que ya escribieron y probaron en las clases 4 a 11. El job no contiene lógica: la ordena.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Batch, streaming, pipeline o job: cada cosa en su lugar
# MAGIC
# MAGIC | Necesito… | En Databricks | En este proyecto |
# MAGIC |---|---|---|
# MAGIC | Transformaciones declarativas con calidad y linaje automáticos | **Lakeflow Declarative Pipelines** | `silver_energia` (clase 5) |
# MAGIC | Encadenar pasos heterogéneos (ingesta, modelos, notebooks, SQL, apps) con reglas de falla | **Lakeflow Jobs** | hoy |
# MAGIC | Procesar a medida que llega, en segundos | streaming / pipeline continuo | no: la fuente publica una vez al día |
# MAGIC | Orquestar sistemas fuera de Databricks (SAP, on-premise, otras nubes) | Azure Data Factory o Airflow, que **llaman** al job | el job sigue siendo la unidad dentro de Databricks |
# MAGIC
# MAGIC Un pipeline puede ser una tarea de un job (`pipeline_task`): en producción, `silver_energia` sería la tarea 2. En Free
# MAGIC Edition solo hay un pipeline activo por cuenta; por eso hoy la calidad corre como notebook con las mismas reglas.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · TODO 1 — `politica()` en `src/xm_demanda/orquestacion/job.py`
# MAGIC
# MAGIC ![Política de fallas](./../img/politica_fallas.png)
# MAGIC
# MAGIC El job ya está definido como un diccionario con el formato de la Jobs API (`definicion_job`). Falta lo que lo hace
# MAGIC confiable: la **política de fallas** de cada tarea.
# MAGIC
# MAGIC | Tarea | Política | Por qué |
# MAGIC |---|---|---|
# MAGIC | `inferencia` | `max_retries = 2`, `min_retry_interval_millis = 60000`, `retry_on_timeout = True` | las fallas transitorias (el modelo tarda en cargar, un nodo se cae) existen, y reintentar es **seguro** porque escribe con MERGE |
# MAGIC | `chequeo` | `run_if = "ALL_DONE"`, `timeout_seconds = 600` | un control que solo corre cuando todo salió bien no controla nada |
# MAGIC | las demás | `timeout_seconds = 1800`, sin reintentos | reintentar una ingesta o un overwrite sin entender la falla esconde el problema |
# MAGIC
# MAGIC Los tests están en `tests/unit/test_orquestacion.py`. Cuando pasen, la celda de abajo imprime el DAG en orden.

# COMMAND ----------

importlib.reload(jobdef)
definicion = jobdef.definicion_job(usuario, ruta_tareas, email)
orden = jobdef.orden_topologico(definicion)
print("Orden de ejecución:", " → ".join(orden))
for t in definicion["tasks"]:
    extra = {k: v for k, v in t.items() if k in ("max_retries", "run_if", "timeout_seconds", "condition_task")}
    print(f"  {t['task_key']:17s} depende de {[d['task_key'] for d in t['depends_on']]!s:24s} {extra}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Crear el job (como código, idempotente)
# MAGIC
# MAGIC Si el job ya existe, se **reemplaza** su configuración (`jobs/reset`); si no, se crea. Correr esta celda dos veces no
# MAGIC crea dos jobs: es la misma idea del MERGE, ahora para la infraestructura. En la clase 13 este diccionario se convierte
# MAGIC en `resources/job_diario.yml` y lo despliega el bundle.

# COMMAND ----------


def api(metodo, ruta, **kw):
    return w.api_client.do(metodo, f"/api/2.2/jobs/{ruta}", **kw)


existentes = [j for j in api("GET", "list", query={"name": definicion["name"]}).get("jobs", []) if j["settings"]["name"] == definicion["name"]]
if existentes:
    job_id = existentes[0]["job_id"]
    api("POST", "reset", body={"job_id": job_id, "new_settings": definicion})
    print("Job actualizado:", job_id)
else:
    job_id = api("POST", "create", body=definicion)["job_id"]
    print("Job creado:", job_id)
print(f"{w.config.host}/jobs/{job_id}")

# COMMAND ----------


def esperar(run_id, max_min=30, tras_reparacion=False):
    """Espera a que el run termine e imprime el avance. Devuelve la respuesta de runs/get.

    Tras un Repair run, primero espera a que el run deje de figurar como terminado.
    """
    if tras_reparacion:
        for _ in range(24):
            if api("GET", "runs/get", query={"run_id": run_id}).get("state", {}).get("life_cycle_state") != "TERMINATED":
                break
            time.sleep(5)
    inicio, visto = time.time(), None
    while time.time() - inicio < max_min * 60:
        run = api("GET", "runs/get", query={"run_id": run_id})
        estado = run.get("state", {})
        vida = estado.get("life_cycle_state") or run.get("status", {}).get("state")
        activas = [t["task_key"] for t in run.get("tasks", []) if t.get("state", {}).get("life_cycle_state") in ("RUNNING", "PENDING", "QUEUED")]
        linea = f"{vida} · {activas}"
        if linea != visto:
            print(f"{(time.time() - inicio) / 60:5.1f} min  {linea}")
            visto = linea
        if vida in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            return run
        time.sleep(15)
    raise TimeoutError(f"El run {run_id} sigue corriendo: míralo en la UI")


def resumen(run):
    r = pd.DataFrame(jobdef.resumen_run(run))
    print("Resultado del run:", run.get("state", {}).get("result_state"), "|", run.get("run_page_url"))
    return r


# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Corrida 1: un día normal
# MAGIC
# MAGIC `fecha_proceso = 2026-07-26`: el día siguiente al último dato publicado. En producción este parámetro sería el valor
# MAGIC dinámico `{{job.start_time.iso_date}}`; aquí lo fijamos porque los datos son históricos. `forzar = si` hace que el
# MAGIC job recalcule aunque bronce no reciba archivos nuevos (sus datos ya estaban cargados desde la clase 4).
# MAGIC
# MAGIC Free Edition corre como máximo **5 tareas a la vez por cuenta**, y todo el grupo comparte la cuenta. Si la corrida queda
# MAGIC en cola (`QUEUED`), no es un error: el job tiene la cola activada y espera su turno. Es la misma razón por la que en
# MAGIC producción se fija `max_concurrent_runs`.

# COMMAND ----------

run1 = api("POST", "run-now", body={"job_id": job_id, "job_parameters": {"fecha_proceso": "2026-07-26", "forzar": "si"}})["run_id"]
r1 = esperar(run1)
display(resumen(r1))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Corrida 2: el archivo del día no llegó
# MAGIC
# MAGIC ![Falla silenciosa](./../img/falla_silenciosa.png)
# MAGIC
# MAGIC Es 27 de julio. XM no ha publicado el archivo del 26. Bronce lee 0 archivos, termina **en verde** y publica
# MAGIC `procesar = no`; la tarea `hay_datos_nuevos` toma la rama `false` y calidad, plata, oro e inferencia quedan **excluidas**.
# MAGIC Sin el chequeo, este job terminaría en verde, el tablero mostraría el pronóstico de ayer como si fuera de hoy y nadie lo
# MAGIC notaría hasta que planeación preguntara. El chequeo corre siempre (`ALL_DONE`) y lo convierte en una falla con un
# MAGIC mensaje que dice qué pasó y qué hacer. Esa falla dispara el correo.

# COMMAND ----------

run2 = api("POST", "run-now", body={"job_id": job_id, "job_parameters": {"fecha_proceso": "2026-07-27", "forzar": "no"}})["run_id"]
r2 = esperar(run2)
display(resumen(r2))
tarea_chequeo = next(t for t in r2["tasks"] if t["task_key"] == "chequeo")
salida = api("GET", "runs/get-output", query={"run_id": tarea_chequeo["run_id"]})
print("\nMensaje del chequeo:\n", salida.get("error", "(sin error)"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Ve a la interfaz** con el enlace del run: el grafo con la rama excluida en gris, el chequeo en rojo, y en la tarea
# MAGIC `bronce` la pestaña de *task values* con `filas_nuevas = 0`. Así se diagnostica en producción: primero el grafo, después
# MAGIC la tarea roja, después las salidas de las tareas verdes que la alimentaron.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Corrida 3: el archivo llega tarde y se repara solo lo necesario
# MAGIC
# MAGIC ![Repair run](./../img/repair_run.png)
# MAGIC
# MAGIC A media mañana llega el archivo del 26 de julio (aquí lo simulamos: copiamos el 19 de julio, mismo día de la semana, con
# MAGIC fecha 26 y publicación del 27, y lo dejamos en landing como lo haría el proceso de descarga). En vez de lanzar el job
# MAGIC completo otra vez, **Repair run** vuelve a correr `bronce` y todo lo que depende de ella, **dentro de la misma corrida**:
# MAGIC el historial queda en un solo lugar, con el intento fallido y el reparado.

# COMMAND ----------

import xm_demanda.ingest.bronze as bronze

landing = f"/Volumes/workspace/c01_{usuario}/raw/landing"
archivo = f"{landing}/demanda_2026-07-27_tardio.csv"
if not os.path.exists(archivo):
    llegada = spark.sql(f"SELECT * FROM {esquema}.demanda_raw_vigente WHERE Fecha = '2026-07-19'").toPandas()
    llegada["Fecha"] = pd.Timestamp("2026-07-26").date()
    llegada["FechaPublicacion"] = pd.Timestamp("2026-07-27").date()
    llegada[[n for n, _ in bronze.COLUMNAS_RAW]].to_csv(archivo, index=False)
print(archivo, "·", len(pd.read_csv(archivo)), "filas")

reparacion = api("POST", "runs/repair", body={"run_id": run2, "rerun_tasks": ["bronce"], "rerun_dependent_tasks": True})
print("Repair:", reparacion)
r3 = esperar(run2, tras_reparacion=True)
display(resumen(r3))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 · Programación, disparadores y alertas: en la interfaz
# MAGIC
# MAGIC El job quedó con un calendario diario a las 6:00 (hora de Bogotá) **pausado**, para no gastar la cuota de Free Edition.
# MAGIC Ábrelo en **Jobs & Pipelines → `[xm-demanda] diario · <usuario>`** y mira, sin cambiar nada:
# MAGIC
# MAGIC | Pestaña | Qué mirar |
# MAGIC |---|---|
# MAGIC | **Runs** | las tres corridas; la segunda con dos intentos (fallido y reparado) |
# MAGIC | **Tasks** | el grafo, la condición, la política de cada tarea (*Retries*, *Timeout*, *Run if*) |
# MAGIC | **Schedules & Triggers** | el calendario pausado y, como alternativa, **File arrival** sobre el volumen de landing: el job arranca cuando llega el archivo, no a una hora |
# MAGIC | **Job notifications** | el correo ante falla; se pueden agregar avisos por **duración** (umbral de tiempo esperado) |
# MAGIC | **Permissions** | quién puede ver, ejecutar o administrar el job. En producción el job corre como *service principal* (*Run as*) |

# COMMAND ----------

# MAGIC %md
# MAGIC **Pregunta 1 (respóndela aquí):** el chequeo usa `run_if = ALL_DONE`. ¿Qué pasaría con la alerta de la corrida 2 si lo
# MAGIC dejaras con el valor por defecto (`ALL_SUCCESS`)? ¿Y si `inferencia` fallara después de sus dos reintentos?
# MAGIC
# MAGIC > Tu respuesta:
# MAGIC
# MAGIC **Pregunta 2 (respóndela aquí):** XM publica a veces el archivo a las 9:00 y a veces a las 13:00. ¿Usarías un calendario a
# MAGIC las 6:00, un disparador por llegada de archivo, o ambos? ¿Qué cambiaría en el parámetro `fecha_proceso` y en el chequeo?
# MAGIC
# MAGIC > Tu respuesta:

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificación
# MAGIC
# MAGIC Cuando todo esté en OK: commit `feat(c12): job diario como código con política de fallas y chequeo de frescura`
# MAGIC (incluye `src/xm_demanda/orquestacion/`), push, PR, CI en verde, merge. Al cierre se libera el tag `s12`.
# MAGIC **Hito 6 abierto**: el pipeline ya corre solo; en las clases 13 y 14 se despliega por ambientes.

# COMMAND ----------


def verificar():
    checks = []
    importlib.reload(jobdef)
    try:
        ok = jobdef.politica("inferencia").get("max_retries") == 2 and jobdef.politica("chequeo").get("run_if") == "ALL_DONE"
        checks.append(("politica() implementada (reintentos en inferencia, ALL_DONE en chequeo)", ok))
    except NotImplementedError:
        checks.append(("politica() implementada", False))
    try:
        trabajos = [j for j in api("GET", "list", query={"name": jobdef.nombre_job(usuario), "expand_tasks": "true"}).get("jobs", [])]
        tareas = {t["task_key"]: t for t in trabajos[0]["settings"]["tasks"]} if trabajos else {}
        checks.append(("Job creado con las 7 tareas del DAG", set(tareas) == set(jobdef.TAREAS)))
        checks.append(("Chequeo con run_if ALL_DONE en el job desplegado", tareas.get("chequeo", {}).get("run_if") == "ALL_DONE"))
        runs = api("GET", "runs/list", query={"job_id": trabajos[0]["job_id"], "limit": 25}).get("runs", []) if trabajos else []
        checks.append(("Al menos una corrida exitosa", any(r.get("state", {}).get("result_state") == "SUCCESS" for r in runs)))
        con_reparacion = [
            r for r in runs
            if len(api("GET", "runs/get", query={"run_id": r["run_id"], "include_history": "true"}).get("repair_history", [])) > 1
        ]
        checks.append(("Una corrida reparada con Repair run", bool(con_reparacion)))
    except Exception as e:  # noqa: BLE001
        checks.append((f"Job y corridas ({type(e).__name__})", False))
    try:
        ultima_oro = str(spark.sql(f"SELECT max(fecha) FROM {esquema}.features_demanda_diaria").first()[0])
        hasta = str(spark.sql(f"SELECT max(fecha_pronostico) FROM {esquema}.pronostico_demanda WHERE alias = 'champion'").first()[0])
        checks.append(("Oro incluye el archivo tardío (26-jul)", ultima_oro >= "2026-07-26"))
        checks.append(("Pronóstico del champion hasta el 2-ago", hasta >= "2026-08-02"))
    except Exception as e:  # noqa: BLE001
        checks.append((f"Tablas ({type(e).__name__})", False))

    print()
    for nombre, ok in checks:
        print(("OK   " if ok else "FALTA") + "  " + nombre)
    print("\nListo: commit y push (incluye src/xm_demanda/orquestacion)." if all(ok for _, ok in checks) else "\nRevisa los puntos marcados FALTA.")


verificar()
