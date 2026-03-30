#!/bin/bash

echo "🧹 Eliminando entorno virtual anterior (si existe)..."
rm -rf venv

echo "🐍 Creando nuevo entorno virtual..."
python3 -m venv venv

echo "⚡ Activando entorno virtual..."
source venv/bin/activate

echo "⬆️ Actualizando pip..."
pip install --upgrade pip

echo "📦 Instalando dependencias..."
pip install psycopg2-binary sqlalchemy pymysql pymongo openpyxl python-dotenv

echo "💾 Guardando dependencias en requirements.txt..."
pip freeze >requirements.txt

echo "Creando el archivo .env para accesos a base de datos"
cp .env.template .env

echo "✅ Instalación completada"
echo "👉 Activa el entorno con: source venv/bin/activate"
