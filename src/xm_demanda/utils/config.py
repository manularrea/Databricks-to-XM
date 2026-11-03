"""Carga y validación de configuración por ambiente (conf/base.yml + conf/<env>.yml).

Regla: la configuración describe DÓNDE y CON QUÉ parámetros corre el código, nunca
credenciales. Los secretos viven en secret scopes (Databricks) o Key Vault (Azure).
"""

from pathlib import Path

import yaml

CONF_DIR = Path(__file__).resolve().parents[3] / "conf"

# Llaves que toda configuración debe traer (base + ambiente).
LLAVES_REQUERIDAS = ("catalog", "schemas", "tablas", "modelo", "volumen_landing")

# Fragmentos de nombre que delatan un secreto escrito en texto plano.
PALABRAS_SECRETO = ("password", "passwd", "token", "secret", "api_key", "apikey")


def load_config(env: str = "dev", conf_dir: Path = CONF_DIR) -> dict:
    """Devuelve la configuración base fusionada con la del ambiente."""
    base = yaml.safe_load((conf_dir / "base.yml").read_text(encoding="utf-8"))
    env_cfg = yaml.safe_load((conf_dir / f"{env}.yml").read_text(encoding="utf-8"))
    return {**base, **env_cfg}


def fqn(cfg: dict, capa: str, tabla: str) -> str:
    """Nombre completo <catalogo>.<esquema>.<tabla> según la configuración."""
    return f"{cfg['catalog']}.{cfg['schemas'][capa]}.{cfg['tablas'][tabla]}"


def _llaves_anidadas(d: dict, prefijo: str = "") -> list[str]:
    """['catalog', 'schemas.bronze', ...]: todas las llaves, con su ruta, para inspeccionarlas."""
    out = []
    for k, v in d.items():
        ruta = f"{prefijo}{k}"
        out.append(ruta)
        if isinstance(v, dict):
            out.extend(_llaves_anidadas(v, ruta + "."))
    return out


def validar_config(cfg: dict) -> None:
    """Lanza ValueError si falta una llave requerida o si alguna llave parece un secreto.

    Pura: no lee archivos ni habla con Spark. Se prueba en tests/unit/test_config.py.
    Ejemplos: una config completa pasa en silencio; {"catalog": "dev", "db_password": "x"}
    lanza ValueError (falta 'schemas' y 'db_password' parece un secreto).
    """
    faltan = [k for k in LLAVES_REQUERIDAS if k not in cfg]
    if faltan:
        raise ValueError(f"Configuración incompleta: falta {faltan}")
    sospechosas = [
        ruta
        for ruta in _llaves_anidadas(cfg)
        if any(p in ruta.lower() for p in PALABRAS_SECRETO)
    ]
    if sospechosas:
        raise ValueError(
            f"Parece un secreto en texto plano: {sospechosas}. Usa un secret scope, no conf/."
        )
