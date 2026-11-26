# AQ-36 : charger des données simulées pour le backfill

Depuis la racine du dépôt, démarrez PostgreSQL et exécutez `python -m scripts.init_db`.
Le générateur écrit dans la base désignée par le fichier `.env`.

```sh
python -m ingestion.history                               # les dates du .env
python -m ingestion.history --days 30                     # les 30 derniers jours UTC terminés
python -m ingestion.history --start 2026-10-27 --end 2026-11-22
```

Les dates sont inclusives et limitées à la veille UTC. Pour le rejeu Airflow, utilisez les mêmes
dates `BACKFILL_START_DATE` et `BACKFILL_END_DATE` dans le `.env` et dans la démonstration.
Avant le 26 novembre, réduisez la date de fin prévue du 25 novembre à la veille du test.

Par défaut, 5 capteurs produisent une mesure toutes les 5 minutes, soit 1 440 mesures par jour.
Le simulateur étiquette les anomalies injectées. Les mesures passent par la validation, les fenêtres
glissantes et les règles existantes, puis sont enregistrées en lots dans PostgreSQL. Kafka n'a
pas besoin de tourner pour ce chargement historique. Les fenêtres sont recalculées à partir de
minuit UTC pour que la même journée puisse être générée indépendamment des autres.

La graine `--seed` rend les mesures reproductibles pour une date et des paramètres donnés.
La partition réservée `-1` et les offsets calculés par heure et capteur préservent les lignes déjà
chargées. Après une interruption, relancez la même commande pour compléter la période. Le bilan
affiche le nombre de nouvelles mesures et anomalies réellement enregistrées.
