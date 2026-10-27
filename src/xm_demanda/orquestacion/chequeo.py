"""Controles de la última tarea del job: ¿lo que se publicó hoy está al día y completo?

Una tarea puede terminar en verde y dejar datos viejos: si la fuente no llegó, la ingesta lee
0 archivos, todo lo demás corre sobre lo de ayer y nadie se entera. Estas reglas convierten esa
falla silenciosa en una falla visible. Puras: se prueban en tests/unit/test_orquestacion.py.
"""

from datetime import date, timedelta


def problemas_de_frescura(
    ultima_fecha_oro: date,
    fecha_proceso: date,
    fechas_pronosticadas: list[date],
    horizonte: int,
) -> list[str]:
    """Lista de problemas (vacía si todo está bien).

    - Oro debe llegar hasta el día anterior a `fecha_proceso`.
    - El pronóstico del champion debe cubrir los `horizonte` días desde `fecha_proceso`.
    """
    problemas = []
    esperada = fecha_proceso - timedelta(days=1)
    if ultima_fecha_oro < esperada:
        atraso = (esperada - ultima_fecha_oro).days
        problemas.append(
            f"Oro llega hasta {ultima_fecha_oro} y para el proceso del {fecha_proceso} se esperaba "
            f"{esperada}: los datos tienen {atraso} día(s) de atraso."
        )
    requeridas = {fecha_proceso + timedelta(days=i) for i in range(horizonte)}
    faltan = sorted(requeridas - set(fechas_pronosticadas))
    if faltan:
        problemas.append(
            f"El pronóstico publicado no cubre {len(faltan)} de los {horizonte} días que necesita "
            f"el consumidor: falta desde {faltan[0]} hasta {faltan[-1]}."
        )
    return problemas
