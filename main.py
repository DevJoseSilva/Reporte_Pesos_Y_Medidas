"""
main.py
=======
Orquestador principal del reporte de dimensiones MercadoLibre.

Uso:
    python main.py
        -> Toma el dia anterior por defecto

    python main.py --inicio 2026-03-01 --fin 2026-03-19
        -> Reporte del 1 al 19 de marzo de 2026

    python main.py --inicio 2026-03-15 --fin 2026-03-15
        -> Reporte de un solo dia

"""

import sys
import argparse
from datetime import date, timedelta, datetime

import psycopg2

from config import OUTPUT
from fuentes import obtener_datos_redshift, obtener_datos_mysql, obtener_datos_mongo
from procesador import procesar
from reporte_excel import generar_excel

import os


# ---------------------------------------------------------------------------
# Parseo de argumentos
# ---------------------------------------------------------------------------

def parsear_argumentos():
    parser = argparse.ArgumentParser(
        description="Genera reporte Excel de cambios de dimensiones MercadoLibre.",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=(
            "Ejemplos:\n"
            "  python main.py\n"
            "      -> Toma el dia anterior por defecto\n\n"
            "  python main.py --inicio 2026-03-01 --fin 2026-03-19\n"
            "      -> Reporte del 1 al 19 de marzo de 2026\n\n"
            "  python main.py --inicio 2026-03-15 --fin 2026-03-15\n"
            "      -> Reporte de un solo dia"
        )
    )
    parser.add_argument(
        "--inicio", type=str, default=None, metavar="YYYY-MM-DD",
        help="Fecha de inicio del periodo (inclusiva). Ejemplo: 2026-03-01"
    )
    parser.add_argument(
        "--fin", type=str, default=None, metavar="YYYY-MM-DD",
        help="Fecha de fin del periodo (inclusiva). Ejemplo: 2026-03-19"
    )
    args = parser.parse_args()

    def parse_fecha(valor, nombre):
        try:
            return datetime.strptime(valor, "%Y-%m-%d").date()
        except ValueError:
            print(f"\n ERROR: Formato de {nombre} incorrecto: '{valor}'")
            print("   Usa el formato YYYY-MM-DD, por ejemplo: 2026-03-01")
            sys.exit(1)

    ayer = date.today() - timedelta(days=1)

    if args.inicio is None and args.fin is None:
        return ayer, ayer

    if args.inicio is None or args.fin is None:
        print("\n ERROR: Debes indicar --inicio y --fin juntos, o no pasar ninguno.")
        sys.exit(1)

    fecha_inicio = parse_fecha(args.inicio, "--inicio")
    fecha_fin    = parse_fecha(args.fin,    "--fin")

    if fecha_inicio > fecha_fin:
        print(f"\n ERROR: --inicio ({fecha_inicio}) no puede ser posterior a --fin ({fecha_fin}).")
        sys.exit(1)

    if fecha_fin > date.today():
        print(f"\n ERROR: --fin ({fecha_fin}) no puede ser una fecha futura.")
        sys.exit(1)

    return fecha_inicio, fecha_fin


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    fecha_inicio, fecha_fin = parsear_argumentos()

    dias = (fecha_fin - fecha_inicio).days + 1
    print("=" * 62)
    print("  REPORTE DIMENSIONES MERCADOLIBRE")
    print(f"  Inicio : {fecha_inicio} 00:00")
    print(f"  Fin    : {fecha_fin} 23:59")
    if dias > 1:
        print(f"  Dias   : {dias}")
    print("=" * 62)

    # 1. Redshift -----------------------------------------------------------
    try:
        nuevos, anteriores = obtener_datos_redshift(fecha_inicio, fecha_fin)
    except psycopg2.OperationalError as e:
        print(f"\n ERROR: No se pudo conectar a Redshift:\n   {e}")
        print("  Verifica las credenciales en config.py")
        sys.exit(1)

    if not nuevos:
        print("\n  No se encontraron cambios para el periodo indicado.")
        sys.exit(0)

    item_ids = list(nuevos.keys())

    # 2. MySQL --------------------------------------------------------------
    info_mysql = {}
    try:
        info_mysql = obtener_datos_mysql(item_ids)
    except Exception as e:
        print(f"\n  ADVERTENCIA MySQL: {e}")
        print("  El reporte continuará sin campos cuenta/proveedor/marca/sku/permalink.\n")

    # 3. MongoDB ------------------------------------------------------------
    info_mongo = {}
    try:
        info_mongo = obtener_datos_mongo(item_ids)
    except Exception as e:
        print(f"\n  ADVERTENCIA MongoDB: {e}")
        print("  El reporte continuará sin campos ventas/fecha_creacion/estado.\n")

    # 4. Procesar -----------------------------------------------------------
    print("\n  Procesando cambios...")
    registros = procesar(nuevos, anteriores, info_mysql, info_mongo)
    print(f"  -> {len(registros)} items con cambio real detectado.")

    conteo = {"ALTA": 0, "MEDIA": 0, "BAJA": 0, "INFO": 0}
    for r in registros:
        conteo[r["alerta"]] += 1
    print(f"     ALTA: {conteo['ALTA']}  MEDIA: {conteo['MEDIA']}  "
          f"BAJA: {conteo['BAJA']}  INFO: {conteo['INFO']}")

    # 5. Excel --------------------------------------------------------------
    print("\n  Generando Excel...")
    ruta = generar_excel(registros, fecha_inicio, fecha_fin, OUTPUT)
    print(f"\n  Reporte generado: {os.path.abspath(ruta)}")
    print("=" * 62)


if __name__ == "__main__":
    main()
