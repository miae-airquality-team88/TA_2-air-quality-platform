# Création des topics Kafka — AQ-10

Depuis la racine du dépôt, installer `requirements.txt`, préparer `.env` à partir
de `.env.example`, puis démarrer Kafka avec Docker Desktop :

```sh
docker compose up -d --wait kafka kafka-ui
python -m scripts.create_topics
python -m scripts.create_topics
```

La seconde exécution ignore les topics existants. Le script ne modifie ni ne
supprime les topics déjà présents. Une erreur de création entraîne un code de
sortie non nul. La configuration du serveur provient de `common/config.py`.

| Topic | Partitions | Réplication locale | Conservation |
|---|---:|---:|---:|
| raw_data | 3 | 1 | 7 jours |
| processed_data | 3 | 1 | 7 jours |
| anomalies | 1 | 1 | 30 jours |

Tests sans broker : `pytest -q tests/test_create_topics.py`.

Pour vérifier réellement les trois topics, leurs partitions et leur rétention,
utiliser un Kafka de test, définir `AQ10_KAFKA_INTEGRATION=1` puis lancer
`pytest -q tests/test_create_topics_integration.py`. Sous PowerShell :

```powershell
$env:AQ10_KAFKA_INTEGRATION = '1'
pytest -q tests/test_create_topics_integration.py
Remove-Item Env:AQ10_KAFKA_INTEGRATION
```

Ce test crée les topics et les conserve. Il détecte aussi une mauvaise
configuration préexistante, sans la modifier. Le workflow GitHub AQ-10 réalise
la même vérification avec le broker du fichier Compose du projet sous Python 3.11.

Pour la capture demandée par le guide, ouvrir http://localhost:8085 et afficher
les trois topics dans Kafka UI. La clôture Jira intervient après validation,
approbation de la PR par un autre membre et fusion dans `main`.
