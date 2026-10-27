"""El job diario como código: definición (Jobs API 2.2), validación del DAG y resumen de runs.

Funciones puras: no hablan con Databricks. El lab 12 las usa para crear el job con el SDK, y la
clase 13 lleva exactamente esta misma estructura a `resources/*.yml` de un bundle.

    bronce ─► hay_datos_nuevos ─(true)─► calidad ─► plata ─► oro ─► inferencia ─► chequeo
                                 └─(false)─ (se excluye la rama)                   ▲
                                                                     run_if ALL_DONE: corre siempre
"""

TAREAS = ["bronce", "hay_datos_nuevos", "calidad", "plata", "oro", "inferencia", "chequeo"]
NOTEBOOKS = {
    "bronce": "t1_bronce",
    "calidad": "t2_calidad",
    "plata": "t3_plata",
    "oro": "t4_oro",
    "inferencia": "t5_inferencia",
    "chequeo": "t6_chequeo",
}
PARAMETROS = {
    "usuario": "",
    "fecha_proceso": "2026-07-26",  # en producción: {{job.start_time.iso_date}}
    "forzar": "si",
    "umbral_cuarentena": "0.01",
}
TIMEOUT_DEFECTO_S = 1800


def nombre_job(usuario: str) -> str:
    return f"[xm-demanda] diario · {usuario}"


def politica(tarea: str) -> dict:
    """Cómo debe comportarse cada tarea ante fallas: reintentos, timeout y condición de ejecución.

    - `inferencia`: es la única que se reintenta (2 veces, 1 minuto entre intentos, también si
      vence el timeout). Es segura de reintentar porque escribe con MERGE idempotente (clase 11).
    - `chequeo`: corre SIEMPRE, aunque lo anterior haya fallado o se haya excluido
      (`run_if = "ALL_DONE"`): un control que solo corre cuando todo salió bien no controla nada.
      Timeout corto: 600 s.
    - las demás: timeout de TIMEOUT_DEFECTO_S, sin reintentos.
    Devuelve las llaves del formato de tarea de la Jobs API (max_retries, min_retry_interval_millis,
    retry_on_timeout, timeout_seconds, run_if).
    """
    # TODO (lab 12, sección 3): implementar. Tres casos: "inferencia", "chequeo" y el resto.
    raise NotImplementedError("politica: completa esta función (lab 12, sección 3)")


def definicion_job(usuario: str, ruta_tareas: str, email: str | None = None) -> dict:
    """Configuración completa del job para `jobs/create` (Jobs API 2.2).

    `ruta_tareas`: carpeta del workspace con los notebooks t1…t6 (sin /Workspace al inicio).
    """

    def notebook(clave: str, depende: list[dict]) -> dict:
        return {
            "task_key": clave,
            "depends_on": depende,
            "notebook_task": {
                "notebook_path": f"{ruta_tareas}/{NOTEBOOKS[clave]}",
                "source": "WORKSPACE",
            },
            **politica(clave),
        }

    tareas = [
        notebook("bronce", []),
        {
            "task_key": "hay_datos_nuevos",
            "depends_on": [{"task_key": "bronce"}],
            "condition_task": {
                "op": "EQUAL_TO",
                "left": "{{tasks.bronce.values.procesar}}",
                "right": "si",
            },
        },
        notebook("calidad", [{"task_key": "hay_datos_nuevos", "outcome": "true"}]),
        notebook("plata", [{"task_key": "calidad"}]),
        notebook("oro", [{"task_key": "plata"}]),
        notebook("inferencia", [{"task_key": "oro"}]),
        notebook("chequeo", [{"task_key": "inferencia"}]),
    ]
    definicion = {
        "name": nombre_job(usuario),
        "tags": {"iniciativa": "xm-demanda", "clase": "12", "usuario": usuario},
        "parameters": [
            {"name": n, "default": usuario if n == "usuario" else v} for n, v in PARAMETROS.items()
        ],
        "tasks": tareas,
        "max_concurrent_runs": 1,
        "queue": {"enabled": True},
        "schedule": {
            "quartz_cron_expression": "0 0 6 * * ?",
            "timezone_id": "America/Bogota",
            "pause_status": "PAUSED",
        },
    }
    if email:
        definicion["email_notifications"] = {"on_failure": [email]}
    return definicion


def orden_topologico(definicion: dict) -> list[str]:
    """Orden de ejecución de las tareas. Lanza ValueError si hay un ciclo o una dependencia rota."""
    tareas = {
        t["task_key"]: [d["task_key"] for d in t.get("depends_on", [])] for t in definicion["tasks"]
    }
    for clave, deps in tareas.items():
        faltan = [d for d in deps if d not in tareas]
        if faltan:
            raise ValueError(f"{clave} depende de tareas que no existen: {faltan}")
    orden, visitadas, en_curso = [], set(), set()

    def visitar(clave):
        if clave in en_curso:
            raise ValueError(f"Ciclo en el DAG que pasa por '{clave}'")
        if clave in visitadas:
            return
        en_curso.add(clave)
        for d in tareas[clave]:
            visitar(d)
        en_curso.discard(clave)
        visitadas.add(clave)
        orden.append(clave)

    for clave in tareas:
        visitar(clave)
    return orden


def estado_tarea(tarea_run: dict) -> str:
    """SUCCESS, FAILED, EXCLUDED, RUNNING… a partir del `state` (o `status`) de un task run."""
    estado = tarea_run.get("state", {})
    if estado.get("result_state"):
        return estado["result_state"]
    status = tarea_run.get("status", {})
    detalle = status.get("termination_details", {}).get("code")
    return detalle or estado.get("life_cycle_state") or status.get("state", "?")


def resumen_run(run: dict) -> list[dict]:
    """Una fila por tarea de un run (respuesta de `jobs/runs/get`): estado, intento, duración."""
    filas = []
    for t in run.get("tasks", []):
        inicio, fin = t.get("start_time") or 0, t.get("end_time") or 0
        filas.append(
            {
                "tarea": t["task_key"],
                "estado": estado_tarea(t),
                "intento": t.get("attempt_number", 0),
                "duracion_s": round((fin - inicio) / 1000) if inicio and fin else None,
                "task_run_id": t.get("run_id"),
            }
        )
    orden = {k: i for i, k in enumerate(TAREAS)}
    return sorted(filas, key=lambda f: (orden.get(f["tarea"], 99), f["intento"]))
