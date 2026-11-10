# Schéma PostgreSQL — AQ-12

Trois tables partagées : `measurements` (mesures nettoyées et traçabilité Kafka),
`anomalies` (détections) et `daily_aggregates` (un capteur par journée).
Les colonnes et index sont définis dans `init.sql`.

## Initialisation

Sur une base neuve, `docker compose up -d postgres` exécute automatiquement
`sql/init.sql` grâce au montage `/docker-entrypoint-initdb.d`.
Sur un volume PostgreSQL déjà initialisé, exécuter depuis la racine :

```sh
python -m scripts.init_db
```

La commande peut être relancée sans effacer les données. Ne pas supprimer le
volume existant pour installer le schéma. Les paramètres de connexion proviennent
du fichier `.env` via `common.config`. Les autres modules utilisent
`common.db.get_connection()` et ferment leur connexion après utilisation.

Les scripts d'entrée Docker s'exécutent uniquement lors de la première
initialisation du volume. Le profil applicatif appelle aussi `scripts.init_db`,
mais le consumer sera livré séparément dans AQ-18.

## Contrats de stockage

- Les dates de mesure et de traçabilité utilisent `TIMESTAMPTZ`.
- Les mesures nettoyées peuvent être `NULL`.
- Une paire Kafka (partition, offset) identifie un message du flux source :
  l'écriture doit conserver les coordonnées de ce même topic source.
- Plusieurs anomalies distinctes peuvent être attachées au même message ;
  le type et le paramètre participent à leur unicité.
- Les agrégats sont uniques par jour et par capteur.
- `CREATE IF NOT EXISTS` initialise le schéma ; il ne remplace pas une migration
  si la structure d'une table déjà existante doit évoluer.

## Tests

```sh
pytest -q tests/test_init_db.py
```

Les tests utilisent exclusivement `airquality_test`, créée avec le compte
PostgreSQL configuré (qui doit pouvoir créer une base). Cette base est réservée
aux tests : ses trois tables sont vidées entre les cas.
La configuration refuse de l'utiliser si c'est aussi la base de travail.
Sans PostgreSQL, les tests sont ignorés localement. Dans le job CI PostgreSQL,
`AQ_REQUIRE_POSTGRES=1` rend une connexion impossible bloquante.
