"""
procesador.py
=============
Responsabilidad: lógica de negocio pura.

  - Helpers numéricos (parse_num, safe_pct, ...)
  - Helpers de formato de periodo
  - procesar() → combina datos de Redshift + MySQL + MongoDB en una
    lista de registros listos para el Excel
"""

import re
from datetime import date


# ---------------------------------------------------------------------------
# Helpers numéricos
# ---------------------------------------------------------------------------


def parse_num(valor):
    if valor is None:
        return None
    m = re.search(r"[\d.]+", str(valor))
    return float(m.group()) if m else None


def safe_pct(nuevo, viejo):
    if not viejo or viejo == 0:
        return None
    return round((nuevo - viejo) / viejo * 100, 1)


def calcular_alerta(pct):
    if pct is None:
        return "INFO"
    a = abs(pct)
    return "ALTA" if a >= 30 else ("MEDIA" if a >= 10 else "BAJA")


def calcular_tendencia(pct):
    if pct is None:
        return "neutral"
    return "alza" if pct > 0 else "baja"


# ---------------------------------------------------------------------------
# Helpers de periodo
# ---------------------------------------------------------------------------


def label_periodo(fecha_inicio, fecha_fin):
    if fecha_inicio == fecha_fin:
        return fecha_inicio.strftime("%d de %B de %Y")
    return (
        f"{fecha_inicio.strftime('%d de %B de %Y')} "
        f"al {fecha_fin.strftime('%d de %B de %Y')}"
    )


def detalle_periodo(fecha_inicio, fecha_fin):
    return (
        f"{fecha_inicio.strftime('%d/%m/%Y')} 00:00"
        f"  ->  {fecha_fin.strftime('%d/%m/%Y')} 23:59"
    )


# ---------------------------------------------------------------------------
# Constantes de alerta
# ---------------------------------------------------------------------------

ALERTA_STYLE = {
    "ALTA": ("FF4D6D", "FFFFFF"),
    "MEDIA": ("F59E0B", "FFFFFF"),
    "BAJA": ("22C55E", "FFFFFF"),
    "INFO": ("60A5FA", "FFFFFF"),
}

ORDER_ALERTA = {"ALTA": 0, "MEDIA": 1, "BAJA": 2, "INFO": 3}


# ---------------------------------------------------------------------------
# Construcción de registros
# ---------------------------------------------------------------------------


def procesar(nuevos, anteriores, info_mysql=None, info_mongo=None):
    """
    Combina los datos de las tres fuentes y devuelve una lista de dicts
    ordenada por nivel de alerta DESC.

    Campos de cada registro:
        item_id, fecha
        cuenta, proveedor, marca, sku_gme, permalink   ← MySQL
        ventas, fecha_creacion, estado                 ← MongoDB
        peso_old/new, alto_old/new, ancho_old/new, largo_old/new
        vol_old/new, fact_old/new, pct_peso, pct_fact
        alerta, tendencia
    """
    if info_mysql is None:
        info_mysql = {}
    if info_mongo is None:
        info_mongo = {}

    registros = []

    for item_id, n in nuevos.items():
        a = anteriores.get(item_id, {})

        peso_new = parse_num(n.get("peso"))
        alto_new = parse_num(n.get("alto"))
        ancho_new = parse_num(n.get("ancho"))
        largo_new = parse_num(n.get("largo"))

        peso_old = parse_num(a.get("peso"))
        alto_old = parse_num(a.get("alto"))
        ancho_old = parse_num(a.get("ancho"))
        largo_old = parse_num(a.get("largo"))

        # Filtrar items sin cambio real
        if (
            peso_new == peso_old
            and alto_new == alto_old
            and ancho_new == ancho_old
            and largo_new == largo_old
        ):
            continue

        vol_new = (
            round(ancho_new * alto_new * largo_new / 5000, 2)
            if all(v is not None for v in [ancho_new, alto_new, largo_new])
            else None
        )
        vol_old = (
            round(ancho_old * alto_old * largo_old / 5000, 2)
            if all(v is not None for v in [ancho_old, alto_old, largo_old])
            else None
        )

        fact_new = max(v for v in [peso_new or 0, vol_new or 0]) or None
        fact_old = max(v for v in [peso_old or 0, vol_old or 0]) or None

        pct_peso = safe_pct(peso_new, peso_old) if (peso_new and peso_old) else None
        pct_fact = safe_pct(fact_new, fact_old) if (fact_new and fact_old) else None

        mysql = info_mysql.get(item_id, {})
        mongo = info_mongo.get(item_id, {})

        registros.append(
            {
                # Identificacion
                "item_id": item_id,
                "fecha": n.get("fecha", ""),
                # MySQL
                "cuenta": mysql.get("cuenta"),
                "proveedor": mysql.get("proveedor"),
                "marca": mysql.get("marca"),
                "sku_gme": mysql.get("sku_gme"),
                "permalink": mysql.get("permalink"),
                # MongoDB
                "ventas": mongo.get("ventas"),
                "fecha_creacion": mongo.get("fecha_creacion"),
                "estado": mongo.get("estado"),
                # Dimensiones
                "peso_old": int(peso_old) if peso_old else None,
                "peso_new": int(peso_new) if peso_new else None,
                "alto_old": alto_old,
                "alto_new": alto_new,
                "ancho_old": ancho_old,
                "ancho_new": ancho_new,
                "largo_old": largo_old,
                "largo_new": largo_new,
                "vol_old": vol_old,
                "vol_new": vol_new,
                "fact_old": fact_old,
                "fact_new": fact_new,
                "pct_peso": pct_peso,
                "pct_fact": pct_fact,
                "alerta": calcular_alerta(pct_fact),
                "tendencia": calcular_tendencia(pct_fact),
            }
        )

    registros.sort(
        key=lambda r: (
            ORDER_ALERTA[r["alerta"]],
            -(abs(r["pct_fact"]) if r["pct_fact"] is not None else 0),
        )
    )
    return registros
