# Databricks notebook source
# MAGIC %md
# MAGIC # Clase 15 · Sesión de cierre: una versión nueva en producción, un modelo que se cuida solo y todo Databricks en un recorrido
# MAGIC
# MAGIC Esta sesión no tiene diapositivas: este notebook es el material. Las secciones teóricas son imágenes y tablas; las
# MAGIC secciones con código consultan lo que de verdad corre en tus ambientes. El trabajo práctico de monitoreo está en
# MAGIC `lab15_monitoreo_reentrenamiento`.
# MAGIC
# MAGIC | # | Bloque | Min | Dónde |
# MAGIC |---|---|---|---|
# MAGIC | 1 | Qué es un release · su v1.0.0 · la v1.1.0 del curso | 8 | aquí, §1 · GitHub |
# MAGIC | 2 | Qué se monitorea (mientras corre el CI) | 10 | aquí, §2 |
# MAGIC | 3 | Aprobar prod: la versión anterior reemplazada | 10 | GitHub · aquí, §3 |
# MAGIC | 4 | Error observado y drift | 18 | `lab15`, secciones 1–3 |
# MAGIC | 5 | La política, el job y el reentrenamiento por evento | 25 | `lab15`, secciones 4–5 |
# MAGIC | — | **Receso** (el candidato entrena) | 10 | — |
# MAGIC | 6 | De principio a fin | 10 | aquí, §4 |
# MAGIC | 7 | Recorrido por Databricks | 15 | aquí, §5 · barra lateral |
# MAGIC | 8 | Mosaic AI | 8 | aquí, §6 · Playground |
# MAGIC | 9 | Cierre | 6 | aquí, §7 |
# MAGIC
# MAGIC ![Recorrido del curso](./../img/recorrido_curso.png)

# COMMAND ----------

dbutils.widgets.text("usuario", "docente")
dbutils.widgets.text("modelo_ia", "")
usuario = dbutils.widgets.get("usuario").strip().lower()
assert usuario, "Escribe tu usuario (el sufijo de tu esquema c01_<usuario>)."

# COMMAND ----------

import os
import sys

import pandas as pd
import yaml
from databricks.sdk import WorkspaceClient


def _raiz_repo():
    d = os.getcwd()
    for _ in range(8):
        if os.path.exists(os.path.join(d, "databricks.yml")):
            return d
        d = os.path.dirname(d)
    raise FileNotFoundError("No encuentro databricks.yml: ejecuta este notebook desde tu Git folder.")


raiz = _raiz_repo()
sys.path.insert(0, os.path.join(raiz, "src"))
sys.dont_write_bytecode = True
import xm_demanda.utils.ambiente as amb

w = WorkspaceClient()
yo = w.current_user.me().user_name
AMBIENTES = ("dev", "qa", "prod")


def jobs_del_bundle(recurso):
    encontrados = {}
    for j in w.jobs.list():
        nombre = (j.settings.name if j.settings else "") or ""
        for a in AMBIENTES:
            if nombre.endswith(f"{recurso} · {usuario} · {a}"):
                encontrados[a] = w.jobs.get(j.job_id)
    return encontrados


def hora(ms):
    return pd.Timestamp(ms, unit="ms", tz="America/Bogota").strftime("%Y-%m-%d %H:%M") if ms else "—"


print("Raíz:", raiz, "| tú:", yo)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Qué es un release
# MAGIC
# MAGIC ![Rama, commit, tag, release](./../img/release_versiones.png)
# MAGIC
# MAGIC **Ahora, en su fork:** **Releases** → **Draft a new release** → tag `v1.0.0` → *Target* `main` → **Generate release notes**
# MAGIC → **Publish release**. Luego **Actions**: la corrida de `v1.0.0`. Cuando `deploy-prod` diga **Waiting**, la aprobamos juntos.
# MAGIC
# MAGIC ### Republicar: la v1.1.0 del curso
# MAGIC
# MAGIC En la producción del curso corre la `v1.0.0` desde ayer. Planeación pidió que el sistema reaccione antes a la
# MAGIC degradación. El cambio va por PR, con pruebas, y llega a producción con un release que reemplaza a la versión anterior.
# MAGIC
# MAGIC ![Republicar](./../img/republicacion.png)
# MAGIC
# MAGIC | Archivo | Qué cambia | Por qué |
# MAGIC |---|---|---|
# MAGIC | `conf/base.yml` | `tolerancia_degradacion: 0.20 → 0.15` | la regla de negocio vive en configuración, no en el código |
# MAGIC | `notebooks/tareas/t7_monitoreo.py` | cada decisión del monitor guarda la tolerancia con la que se tomó | dentro de seis meses alguien va a preguntar con qué regla se decidió |
# MAGIC | `tests/unit/test_monitoreo.py` | el test lee el umbral de la configuración | un test que repite el número se rompe con cada decisión de negocio |
# MAGIC | `docs/runbook.md` | el SLO de calidad pasa de 5,8 % a 5,5 % | la promesa al negocio cambia con la regla |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Qué se monitorea en un modelo en producción
# MAGIC
# MAGIC Un modelo de demanda casi nunca falla en rojo: se degrada en silencio. El job sigue verde, los datos llegan frescos, y el
# MAGIC pronóstico empeora porque el mundo cambió.
# MAGIC
# MAGIC ![Mapa del monitoreo](./../img/mapa_monitoreo.png)
# MAGIC
# MAGIC ![Qué se monitorea](./../img/que_monitorear.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Error observado: WAPE y sesgo
# MAGIC
# MAGIC ![Error y sesgo](./../img/error_sesgo.png)
# MAGIC
# MAGIC ### Drift: ¿el mundo de hoy se parece al del entrenamiento?
# MAGIC
# MAGIC ![PSI y drift](./../img/psi_drift.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Qué ofrece Databricks, y qué construimos
# MAGIC
# MAGIC | Pieza de Databricks | Qué hace | En XM |
# MAGIC |---|---|---|
# MAGIC | *Data quality monitoring* → **anomaly detection** | frescura y completitud de todas las tablas de un esquema, con un clic (Catalog → esquema → *Details*) | encenderlo desde el primer día |
# MAGIC | *Data quality monitoring* → **data profiling** (antes Lakehouse Monitoring) | tablas `_profile_metrics` y `_drift_metrics`; para modelos, perfiles **InferenceLog** de regresión | junto a nuestro monitor, no en su lugar |
# MAGIC | Nuestro monitor (`src/xm_demanda/monitoreo`) | error, drift y la decisión de reentrenar, como código con pruebas | corre igual en Free Edition y en Azure; la decisión no depende de abrir un tablero |
# MAGIC
# MAGIC ![El monitor dentro del job](./../img/monitor_en_job.png)
# MAGIC
# MAGIC ![Reentrenar no es promover](./../img/politica_reentrenamiento.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Aprobar prod: ¿quedó reemplazada la versión anterior?
# MAGIC
# MAGIC Corre estas dos celdas cuando `deploy-prod` del release esté en verde.
# MAGIC
# MAGIC Dos fuentes que no dependen de lo que diga GitHub. El tag `git_sha` de cada job (lo escribe el pipeline con
# MAGIC `BUNDLE_VAR_git_sha`) y el archivo `conf/base.yml` **desplegado**: en qa y prod, la copia que el bundle dejó en
# MAGIC `.bundle/xm-demanda/<ambiente>/files`; en dev, tu Git folder. Si una versión nueva reemplazó a la anterior, las dos
# MAGIC columnas cambian juntas.

# COMMAND ----------

def conf_desplegada(a):
    if a == "dev":
        ruta = os.path.join(raiz, "conf", "base.yml")
    else:
        ruta = f"/Workspace/Users/{yo}/.bundle/xm-demanda/{a}/files/conf/base.yml"
    with open(ruta, encoding="utf-8") as f:
        return yaml.safe_load(f)


diario = jobs_del_bundle("diario")
filas = []
for a in AMBIENTES:
    job = diario.get(a)
    try:
        tol = conf_desplegada(a)["monitoreo"]["tolerancia_degradacion"]
    except Exception as e:  # noqa: BLE001
        tol = f"(no leído: {type(e).__name__})"
    filas.append({
        "ambiente": a,
        "git_sha": ((job.settings.tags or {}).get("git_sha", "—")[:10]) if job else "(sin job)",
        "tolerancia_degradacion": tol,
        "copia desplegada": "Git folder" if a == "dev" else f".bundle/xm-demanda/{a}/files",
    })
display(pd.DataFrame(filas))

# COMMAND ----------

u_prod = amb.ubicacion(usuario, "prod")
decisiones = spark.table(f"{u_prod['fqn']}.monitoreo_decisiones").toPandas().sort_values("calculado_en", ascending=False)
columnas = [c for c in ("calculado_en", "modelo_version", "dias_desde_entrenamiento", "psi_max", "tolerancia_degradacion",
                        "reentrenar", "motivos") if c in decisiones.columns]
print(f"Últimas decisiones del monitor en {u_prod['fqn']} (una por corrida del job de prod):")
display(decisiones[columnas].head(4))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ahora, el lab
# MAGIC
# MAGIC Abre `lab15_monitoreo_reentrenamiento`: secciones 1 a 5. Al lanzar el reentrenamiento por evento, **receso de 10 minutos**:
# MAGIC el candidato entrena mientras tanto. Al volver, sección 4 de este notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · De principio a fin
# MAGIC
# MAGIC ![El engranaje completo](./../img/engranaje.png)
# MAGIC
# MAGIC La última corrida del job de prod: cada tarea con su estado y su duración, en el orden en que corrió. `EXCLUDED` no es un error: es una rama que el
# MAGIC propio job decidió no correr (sin datos nuevos, o sin motivo para reentrenar).

# COMMAND ----------

def ultima_corrida(job):
    corridas = list(w.jobs.list_runs(job_id=job.job_id, limit=1))
    return w.jobs.get_run(corridas[0].run_id) if corridas else None


r = ultima_corrida(diario["prod"])
assert r, "El job de prod todavía no tiene corridas: publica un release y apruébalo."
print(f"diario · prod · {hora(r.start_time)} · {r.state.result_state.value if r.state.result_state else r.state.life_cycle_state.value}"
      f" · disparo {r.trigger.value if r.trigger else '—'}")
print(r.run_page_url, "\n")
for t in sorted(r.tasks, key=lambda t: (t.start_time or 0, t.task_key)):
    estado = t.state.result_state.value if t.state and t.state.result_state else "—"
    seg = (t.end_time - t.start_time) / 1000 if t.start_time and t.end_time else 0
    print(f"  {t.task_key:19s} {estado:9s} {seg:5.0f} s")

reent = jobs_del_bundle("reentrenamiento")
for a in AMBIENTES:
    if a in reent:
        rr = ultima_corrida(reent[a])
        txt = (f"{hora(rr.start_time)} · {rr.state.result_state.value if rr.state.result_state else rr.state.life_cycle_state.value}"
               if rr else "sin corridas")
        print(f"\nreentrenamiento · {a}: {txt}")

# COMMAND ----------

u_dev = amb.ubicacion(usuario, "dev")
T_DM = f"{u_dev['fqn']}.decisiones_modelo"
if spark.catalog.tableExists(T_DM):
    print("La última decisión de promoción en dev (job de reentrenamiento → evaluar_promover):")
    display(spark.sql(f"SELECT * FROM {T_DM} ORDER BY decidido_en DESC LIMIT 3"))
else:
    print("Todavía no hay decisiones de promoción en dev: corre el reentrenamiento del lab 15.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Recorrido por Databricks: el curso en siete paradas
# MAGIC
# MAGIC ![Mapa de Databricks](./../img/mapa_databricks.png)
# MAGIC
# MAGIC | Parada | Barra lateral | Qué mostrar | Clase |
# MAGIC |---|---|---|---|
# MAGIC | 1 | **Workspace** | el Git folder y, al lado, `.bundle/xm-demanda/prod`: tu trabajo y lo que corre en producción | 0 · 8 · 13 |
# MAGIC | 2 | **Catalog** | `c01_<usuario>`, `_qa`, `_prod`: tablas, volúmenes, modelos; en una tabla, *Lineage* y *Permissions* | 1–7 · 10 |
# MAGIC | 3 | **Jobs & Pipelines** | el job diario de prod: grafo, matriz de corridas, la rama excluida | 12–15 |
# MAGIC | 4 | **Compute** | serverless, *SQL warehouses* y la pestaña *Apps* con la app de la clase 11 | 1 · 11 |
# MAGIC | 5 | **SQL Editor · Dashboards · Genie Agents · Alerts** | la cara para el negocio | 11 · 12 · 15 |
# MAGIC | 6 | **Experiments · Models · Serving** | el ciclo del modelo, y hoy el candidato con su motivo | 9 · 10 · 11 · 15 |
# MAGIC | 7 | **Marketplace** | datos de terceros y Delta Sharing | lo que sigue |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Mosaic AI: lo que sigue
# MAGIC
# MAGIC ![Mosaic AI](./../img/mosaic_ai.png)
# MAGIC
# MAGIC **En vivo:** barra lateral → **Playground** → *System prompt*: "Eres analista de demanda de energía en Colombia. Responde en
# MAGIC tres frases." → "¿Qué puede hacer que un modelo de pronóstico de demanda eléctrica se degrade en un año de El Niño?" →
# MAGIC ícono **⊕** → un segundo modelo → la misma pregunta. Luego **AI Gateway** → **Models**, y **Agents**.
# MAGIC
# MAGIC ### `ai_query`: un modelo de lenguaje desde SQL
# MAGIC
# MAGIC Le pedimos que traduzca la última decisión del monitor a un mensaje para planeación. Los modelos están en **AI Gateway**
# MAGIC → **Models** (`system.ai.<modelo>`); si quieres otro, escríbelo en el widget `modelo_ia`.

# COMMAND ----------

modelo_ia = dbutils.widgets.get("modelo_ia").strip()
# Desde octubre de 2026 los modelos fundacionales se sirven en Unity Gateway como system.ai.<modelo>.
candidatos = [modelo_ia] if modelo_ia else [
    "system.ai.meta-llama-3-3-70b-instruct", "system.ai.gpt-oss-120b", "system.ai.qwen3-next-80b-a3b-instruct"]
modelo_ia = ""
for m in candidatos:
    try:
        spark.sql(f"SELECT ai_query('{m}', 'Responde solo: ok') AS r").first()
        modelo_ia = m
        break
    except Exception as e:  # noqa: BLE001
        print(f"{m}: no disponible ({type(e).__name__})")
assert modelo_ia, "Ningún modelo respondió: revisa AI Gateway → Models, o usa el Playground."
print("Uso:", modelo_ia)

# COMMAND ----------

instruccion = ("Eres analista del pronóstico de demanda de energía de Colombia. Con estos datos del monitor del modelo, "
               "escribe tres frases en español para el equipo de planeación: cómo está el pronóstico, si hay que hacer algo "
               "y por qué. Responde solo con las tres frases, sin tecnicismos y sin inventar cifras. Datos: ")
consulta = f"""
SELECT ai_query('{modelo_ia}', concat('{instruccion}', to_json(struct(*)))) AS mensaje_para_planeacion
FROM (
  SELECT fecha_datos, ambiente, modelo_version,
         round(wape_reciente, 4) AS wape_7_dias, round(wape_referencia, 4) AS wape_prometido_en_validacion,
         round(psi_max, 3) AS psi_maximo, dias_desde_entrenamiento, reentrenar, motivos
  FROM {u_dev['fqn']}.monitoreo_decisiones
  ORDER BY calculado_en DESC
  LIMIT 1
)
"""
display(spark.sql(consulta))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Cierre: lo que se llevan
# MAGIC
# MAGIC | Hito | Qué demuestra | Dónde está |
# MAGIC |---|---|---|
# MAGIC | H1 Pregunta analítica | qué se pronostica, para quién y con qué granularidad | `docs/architecture.md`, ADR-001 |
# MAGIC | H2 Arquitectura y fuentes | bronce idempotente con republicaciones | `src/xm_demanda/ingest`, ADR-002 |
# MAGIC | H3 Gobierno y calidad | permisos, reglas, cuarentena, plata | `docs/governance.md`, `quality`, `silver` |
# MAGIC | H4 Pipeline y oro | features con contrato y pruebas en CI | `features`, `tests/` |
# MAGIC | H5 Modelo y despliegue | experimentos comparables, registro con alias, batch, endpoint y app | `model`, `inference`, `app/` |
# MAGIC | H6 Operación | bundle por ambientes, CI/CD con aprobación, releases, monitor y reentrenamiento | `databricks.yml`, `.github/`, `monitoreo`, `docs/runbook.md` |
# MAGIC
# MAGIC Lo que se llevan no es el modelo de demanda. Es la forma: código en `src/` con pruebas, configuración por ambiente,
# MAGIC tareas delgadas, infraestructura declarada, promoción con compuertas, versiones con nombre, y decisiones sobre el modelo
# MAGIC que quedan escritas. `docs/backlog.md` recoge lo que sigue.
# MAGIC
# MAGIC **Gracias.**
