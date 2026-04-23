"""
fuentes.py
==========
Responsabilidad: obtener datos crudos de cada fuente.

  - Redshift  → datos de dimensiones (nuevos y anteriores)
  - CSV temp  → archivo intermedio compartido por MySQL y MongoDB
  - MySQL     → cuenta, proveedor, marca, sku_gme, permalink
  - MongoDB   → sold_quantity, start_time, status
"""

import csv
import os

import psycopg2
from sqlalchemy import create_engine, text
from pymongo import MongoClient

from config import (
    REDSHIFT,
    TABLA_REDSHIFT,
    MYSQL,
    MONGO,
    TEMP_DIR,
    TEMP_CSV,
)


# ---------------------------------------------------------------------------
# 1. REDSHIFT
# ---------------------------------------------------------------------------


def obtener_datos_redshift(fecha_inicio, fecha_fin):
    """
    Devuelve dos dicts:
      nuevos    = { item_id: {fecha, peso, alto, ancho, largo} }
      anteriores = { item_id: {peso, alto, ancho, largo} }
    """
    print("  Conectando a Redshift...")
    conn = psycopg2.connect(**REDSHIFT)
    cur = conn.cursor()

    print(f"  Consultando registros nuevos ({fecha_inicio} -> {fecha_fin})...")
    cur.execute(f"""
        SELECT
            item_id,
            fecha_insercion,
            seller_package_weight,
            seller_package_height,
            seller_package_width,
            seller_package_length
        FROM {TABLA_REDSHIFT}
        WHERE ultimo = 1
          AND CAST(fecha_insercion AS DATE) BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
    """)
    nuevos = {}
    for row in cur.fetchall():
        item_id = row[0]
        if item_id not in nuevos or str(row[1]) > str(nuevos[item_id]["fecha"]):
            nuevos[item_id] = {
                "fecha": str(row[1]),
                "peso": row[2],
                "alto": row[3],
                "ancho": row[4],
                "largo": row[5],
            }
    print(f"  -> {len(nuevos)} items con cambios detectados.")

    if not nuevos:
        cur.close()
        conn.close()
        return {}, {}

    print("  Consultando registros anteriores...")
    ids_sql = ", ".join(f"'{i}'" for i in nuevos.keys())
    cur.execute(f"""
        SELECT
            t.item_id,
            t.seller_package_weight,
            t.seller_package_height,
            t.seller_package_width,
            t.seller_package_length
        FROM {TABLA_REDSHIFT} t
        INNER JOIN (
            SELECT item_id, MAX(id) AS max_id
            FROM {TABLA_REDSHIFT}
            WHERE ultimo = 0
              AND item_id IN ({ids_sql})
            GROUP BY item_id
        ) sub
        ON t.item_id = sub.item_id
        AND t.id = sub.max_id
    """)
    anteriores = {}
    for row in cur.fetchall():
        anteriores[row[0]] = {
            "peso": row[1],
            "alto": row[2],
            "ancho": row[3],
            "largo": row[4],
        }

    cur.close()
    conn.close()
    print(f"  -> {len(anteriores)} items con historial previo para comparar.")
    return nuevos, anteriores


# ---------------------------------------------------------------------------
# 2. CSV temporal  (compartido por MySQL y MongoDB)
# ---------------------------------------------------------------------------


def exportar_ids_csv(item_ids):
    """Guarda los item_id en un CSV de una sola columna."""
    os.makedirs(TEMP_DIR, exist_ok=True)
    with open(TEMP_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, lineterminator="\r\n")
        for item_id in item_ids:
            writer.writerow([item_id])
    print(f"  -> CSV temporal guardado: {TEMP_CSV}  ({len(item_ids)} registros)")


# ---------------------------------------------------------------------------
# 3. MYSQL  → cuenta, proveedor, marca, sku_gme, permalink
# ---------------------------------------------------------------------------


def obtener_datos_mysql(item_ids):
    """
    Carga el CSV en una tabla temporal de MySQL y ejecuta las consultas
    de enriquecimiento.

    Devuelve: { item_id: {cuenta, proveedor, marca, sku_gme, permalink} }
    """
    exportar_ids_csv(item_ids)

    print("  Conectando a MySQL...")
    url = (
        f"mysql+pymysql://{MYSQL['user']}:{MYSQL['password']}"
        f"@{MYSQL['host']}:{MYSQL['port']}/{MYSQL['dbname']}"
        f"?local_infile=1"
    )
    engine = create_engine(url)
    csv_path_mysql = TEMP_CSV.replace("\\", "/")

    info = {}
    with engine.connect() as conn:
        conn.execute(text("DROP TEMPORARY TABLE IF EXISTS tmp_mlm"))
        conn.execute(
            text("""
            CREATE TEMPORARY TABLE tmp_mlm (
            mlm VARCHAR(100)
            )
        """)
        )
        conn.execute(
            text(f"""
            LOAD DATA LOCAL INFILE '{csv_path_mysql}'
            INTO TABLE tmp_mlm
            LINES TERMINATED BY '\\r\\n'
            (mlm)
        """)
        )
        conn.execute(text("CREATE INDEX IDX_MLM ON tmp_mlm (mlm)"))

        # ! Tabla temporal para info de las mlm
        conn.execute(text("DROP TEMPORARY TABLE IF EXISTS tp_mlm_publicados"))
        conn.execute(
            text("""
            create temporary table tp_mlm_publicados(
                arc_id_en_canal varchar(100),
                arc_art_id int,
                ctc_nombre varchar(250)
            )
        """)
        )
        conn.execute(text("""
            insert INTO tp_mlm_publicados (arc_id_en_canal, arc_art_id, ctc_nombre)
            SELECT apcc.arc_id_en_canal, 
                apcc.arc_art_id, 
                cc.ctc_nombre
            FROM tmp_mlm tp1
            INNER JOIN articulos_publicados_cuentas_canales apcc
                ON tp1.mlm = apcc.arc_id_en_canal AND apcc.arc_eliminado IS NULL
            LEFT JOIN cuentas_canales cc
                ON cc.ctc_id = apcc.arc_ctc_id AND cc.ctc_eliminado IS NULL
        """))
        conn.execute(
            text("CREATE INDEX IDX_ARC_ID_EN_CANAL ON tp_mlm_publicados (arc_id_en_canal)")
        )
        
        # ! Tabla temporal para la info de skus unicos de cada articulo publicado
        conn.execute(text("drop temporary table if exists tp_sku_distinto"))
        conn.execute(
            text("""
            create temporary table tp_sku_distinto(
                tp_art_id int,
                tp_sku varchar(250)
            )
            """)
        )
        conn.execute(
            text("""
            insert into tp_sku_distinto
            select DISTINCT arc_art_id,
                arc_art_id
            from tp_mlm_publicados
            """)
        )
        conn.execute(
            text("CREATE INDEX IDX_SKU ON tp_sku_distinto(tp_art_id, tp_sku);")
        )
        
        # ! Tabla temporal para la info de skus con proveedor, marca
        conn.execute(text("DROP TEMPORARY TABLE IF EXISTS tp_skus"))
        conn.execute(
            text("""
            create temporary table tp_skus(
                apv_art_id int,
                apv_sku varchar(250),
                prv_nombre varchar(150),
                mar_nombre varchar(250),
                tpa_id int null
            )
            """)
        )
        conn.execute(
            text("""
            insert into tp_skus (apv_art_id, apv_sku, prv_nombre, mar_nombre, tpa_id)
            SELECT
                ap.apv_art_id,
                ap.apv_sku,
                p.prv_nombre,
                m.mar_nombre,
                a.art_tpa_id
            FROM articulos_proveedores ap
            INNER JOIN tp_sku_distinto tp1
                ON ap.apv_art_id = tp1.tp_sku
               AND ap.apv_principal = 1
               AND ap.apv_eliminado IS NULL
            INNER JOIN proveedores p ON p.prv_id = ap.apv_prv_id AND p.prv_eliminado IS NULL
            INNER JOIN articulos a   ON a.art_id  = tp1.tp_art_id AND a.art_eliminado IS NULL
            INNER JOIN marcas m      ON a.art_mar_id = m.mar_id    AND m.mar_eliminado IS NULL
            """)
        )
        conn.execute(
            text("CREATE INDEX IDX_ART_SKU ON tp_skus(apv_art_id, apv_sku, tpa_id);")
        )

        result = conn.execute(
            text("""
            SELECT
                arc_id_en_canal AS id_en_canal,
                ctc_nombre      AS cuenta,
                prv_nombre      AS proveedor,
                mar_nombre      AS marca,
                apv_sku         AS sku_gme,
                CONCAT(
                    'https://www.mercadolibre.com.mx/publicaciones/listado?page=1&search=',
                    arc_id_en_canal,
                    '&sort=DEFAULT'
                ) AS permalink
            FROM tp_mlm_publicados tp1
            LEFT JOIN tp_skus tp2 ON tp1.arc_art_id = tp2.apv_art_id
        """)
        )

        for row in result.mappings():
            info[row["id_en_canal"]] = {
                "cuenta": row["cuenta"],
                "proveedor": row["proveedor"],
                "marca": row["marca"],
                "sku_gme": row["sku_gme"],
                "permalink": row["permalink"],
                "skus_hijos": [],   # se llena en la siguiente consulta
            }

        # --- Skus hijos de ensambles (solo donde tpa_id = 2) ----------------
        # Agrupa por sku_padre para armar la lista de hijos por item
        result_ens = conn.execute(
            text("""
            SELECT
                tp1.apv_sku  AS sku_padre,
                ap.apv_sku   AS sku_hijo
            FROM tp_skus tp1
            INNER JOIN ensambles e
                ON tp1.apv_art_id = e.ens_art_padre
               AND e.ens_eliminado IS NULL
            JOIN articulos_proveedores ap
                ON e.ens_art_hijo = ap.apv_art_id
               AND ap.apv_principal = 1
               AND ap.apv_eliminado IS NULL
            WHERE tp1.tpa_id = 2
            ORDER BY tp1.apv_sku, ap.apv_sku
        """)
        )

        # Mapa auxiliar: sku_padre → [sku_hijo, ...]
        hijos_por_sku = {}
        for row in result_ens.mappings():
            padre = row["sku_padre"]
            hijos_por_sku.setdefault(padre, []).append(row["sku_hijo"])

        # Asignar los hijos a cada item usando su sku_gme como llave
        for id_en_canal, datos in info.items():
            sku = datos.get("sku_gme")
            if sku and sku in hijos_por_sku:
                datos["skus_hijos"] = hijos_por_sku[sku]

    print(f"  -> {len(info)} registros obtenidos de MySQL.")
    return info


# ---------------------------------------------------------------------------
# 4. MONGODB  → sold_quantity, start_time, status
# ---------------------------------------------------------------------------


def obtener_datos_mongo(item_ids):
    """
    Consulta la coleccion de MongoDB buscando por el campo "id" (= MLM).
    Aprovecha el CSV ya generado; la lista de IDs viene directamente del
    parametro para no depender del orden de ejecucion.

    Devuelve: { item_id: {ventas, fecha_creacion, estado} }
    """
    print("  Conectando a MongoDB...")
    client = MongoClient(MONGO["uri"])
    db = client[MONGO["database"]]
    col = db[MONGO["collection"]]

    # Consulta en un solo round-trip usando $in sobre el indice del campo "id"
    cursor = col.find(
        {"id": {"$in": list(item_ids)}},
        {"_id": 0, "id": 1, "sold_quantity": 1, "start_time": 1, "status": 1},
    )

    info = {}
    for doc in cursor:
        mlm = doc.get("id")
        if not mlm:
            continue

        # start_time puede venir como datetime o como string ISO
        start_time = doc.get("start_time")
        if hasattr(start_time, "strftime"):
            start_time = start_time.strftime("%Y-%m-%d %H:%M")
        elif isinstance(start_time, str) and "T" in start_time:
            # "2023-05-12T10:30:00.000Z" → "2023-05-12 10:30"
            start_time = start_time[:16].replace("T", " ")

        info[mlm] = {
            "ventas": doc.get("sold_quantity"),
            "fecha_creacion": start_time,
            "estado": doc.get("status"),
        }

    client.close()
    print(f"  -> {len(info)} registros obtenidos de MongoDB.")
    return info