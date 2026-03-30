"""
config.py
=========
Credenciales y constantes globales del proyecto.
Edita SOLO este archivo para cambiar conexiones o rutas.

Dependencias:
    pip install psycopg2-binary sqlalchemy pymysql pymongo openpyxl python-dotenv
"""

import os
from urllib.parse import quote_plus
from dotenv import load_dotenv

# Cargar variables del .env
load_dotenv()

# ---------------------------------------------------------------------------
# REDSHIFT
# ---------------------------------------------------------------------------
REDSHIFT = {
    "host": os.getenv("REDSHIFT_HOST"),
    "port": int(os.getenv("REDSHIFT_PORT", 5439)),
    "dbname": os.getenv("REDSHIFT_DBNAME"),
    "user": os.getenv("REDSHIFT_USER"),
    "password": os.getenv("REDSHIFT_PASSWORD"),
}

TABLA_REDSHIFT = "items_dimensiones_historico"

# ---------------------------------------------------------------------------
# MYSQL
# ---------------------------------------------------------------------------
MYSQL = {
    "host": os.getenv("MYSQL_HOST"),
    "port": int(os.getenv("MYSQL_PORT", 3306)),
    "dbname": os.getenv("MYSQL_DBNAME"),
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
}

# ---------------------------------------------------------------------------
# MONGODB
# ---------------------------------------------------------------------------
user = quote_plus(os.getenv("MONGO_USER"))
password = quote_plus(os.getenv("MONGO_PASSWORD"))
host = os.getenv("MONGO_HOST")
MONGO = {
    "uri": f"mongodb+srv://{user}:{password}@{host}/",  # ajusta con tu URI real
    "database": os.getenv("MONGO_DATABASE"),  # nombre de la BD en Mongo
    "collection": os.getenv("MONGO_COLLECTION"),  # coleccion que tiene el campo "id" = mlm
}

# ---------------------------------------------------------------------------
# RUTAS
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMP_DIR = os.path.join(BASE_DIR, "temp")
TEMP_CSV = os.path.join(TEMP_DIR, "tp_mlm_dimensiones.csv")
OUTPUT = os.path.join(BASE_DIR, "excel")  # carpeta donde se guarda el Excel final
