"""Configuration commune du projet.

Toutes les adresses et tous les secrets sont lus depuis le fichier .env
(copié depuis .env.example). Aucun module ne doit écrire une adresse en dur :
il importe les valeurs depuis ce fichier.

Exemple :
    from common.config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_RAW
"""
import os
from pathlib import Path

from dotenv import load_dotenv

# Racine du dépôt (le dossier qui contient .env)
ROOT_DIR = Path(__file__).resolve().parent.parent
load_dotenv(ROOT_DIR / ".env")

# --- Kafka ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_RAW = "raw_data"
TOPIC_PROCESSED = "processed_data"
TOPIC_ANOMALIES = "anomalies"

# --- PostgreSQL ---
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5433"))
POSTGRES_USER = os.getenv("POSTGRES_USER", "airquality")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")
POSTGRES_DB = os.getenv("POSTGRES_DB", "airquality")

# --- OpenAQ ---
OPENAQ_API_KEY = os.getenv("OPENAQ_API_KEY", "")
OPENAQ_LOCATION_ID = int(os.getenv("OPENAQ_LOCATION_ID", "3847229"))

# --- Chemins ---
DATA_DIR = ROOT_DIR / "data"
SCHEMA_PATH = ROOT_DIR / "schemas" / "sensor_message.json"
