# air-quality-platform

Plateforme DataOps/MLOps de détection d’anomalies de qualité de l’air

## Équipe
- Zakaria Daoudi :zakaria.daoudi.iae@gmail.com
- Ismail Bouabid : ismailbouabid@gmail.com
- Aziz Agourram :aziz19021982@gmail.com
- Dris Alamass : dra444706@gmail.com
- Abderrahim Elharragui: abderrahim.elharragui@gmail.com


## Démarrage rapide

1. Copier la configuration : `cp .env.example .env`
2. Démarrer l'infrastructure : `docker compose up -d`
3. Vérifier : `docker compose ps` (Kafka et PostgreSQL doivent être *healthy*)

| Service | Adresse |
|---|---|
| Kafka (depuis Windows) | `localhost:9092` |
| Kafka (depuis un conteneur) | `kafka:29092` |
| PostgreSQL | `localhost:5433` |
| Kafka UI | http://localhost:8085 |



