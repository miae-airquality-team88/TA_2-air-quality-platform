# =============================================================
#  Image de l'application Python (AQ-7)
#  Une seule image pour tous les programmes du projet :
#    - consumer streaming  : python -m streaming.consumer
#    - simulateur/producer : python -m ingestion.producer
#  Le programme lancé est choisi par la commande (voir docker-compose.yml).
# =============================================================
FROM python:3.11-slim

WORKDIR /app

# Pas de fichiers .pyc, affichage immédiat des print() dans les logs Docker
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# 1. Dépendances d'abord : cette couche est réutilisée tant que requirements.txt ne change pas
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 2. Puis le code
COPY common ./common
COPY schemas ./schemas
COPY ingestion ./ingestion
COPY streaming ./streaming
COPY scripts ./scripts
COPY sql ./sql

CMD ["python", "-m", "streaming.consumer"]
