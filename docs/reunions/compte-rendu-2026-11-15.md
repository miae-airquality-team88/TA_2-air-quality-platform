# Compte rendu — Réunion n°5 : revue du Sprint 2 et planification du Sprint 3

|||
|-|-|
|**Date**|Dimanche 15 novembre 2026|
|**Type**|Revue de sprint + démonstration + rétrospective + planification|
|**Animateur (Scrum Master)**|Zakaria Daoudi|
|**Participants**|Zakaria Daoudi, Ismail Bouabid, Dris Alamass, Abderrahim Elharragui, Aziz Agourram|
|**Sprint concerné**|AQ Sprint 2 — 9 au 16 novembre 2026|
|**Ticket Jira**|AQ-26|

\---

## 1\. Objectif du Sprint 2

> Consumer streaming opérationnel : mesures validées, agrégées, anomalies détectées et stockées
> dans PostgreSQL. CI GitHub Actions active.

**Résultat : objectif atteint — 29 points terminés sur 29 (100 %).**

## 2\. Tickets du Sprint 2

|Ticket|Description|Responsable|Points|PR|Statut|
|-|-|-|-|-|-|
|AQ-7|CI GitHub Actions : lint + tests + build Docker|Zakaria|3|#23|Terminé|
|AQ-12|Schéma PostgreSQL : temps réel + agrégats quotidiens|Dris|3|#24|Terminé|
|AQ-13|Validation des mesures (Pydantic) + tests|Dris|5|#27|Terminé|
|AQ-19|Publication des mesures nettoyées dans `processed\\\_data`|Ismail|2|#26|Terminé|
|AQ-20|Fenêtres glissantes PM2.5/PM10 sur 5 et 15 min|Aziz|5|#25|Terminé|
|AQ-21|Règles métier : seuils OMS, pics, capteur bloqué → `anomalies`|Abderrahim|3|#28|Terminé|
|AQ-22|Écriture par lots et agrégats quotidiens dans PostgreSQL|Abderrahim|3|#29|Terminé|
|AQ-18|Consumer streaming : `raw\\\_data` → `processed\\\_data`, `anomalies`, PostgreSQL|Ismail|3|#30|Terminé|
|AQ-23|Mesure de la latence Kafka → PostgreSQL|Zakaria|2|#31|Terminé|
||**Total**||**29 / 29**|||

Toutes les PR ont été vérifiées par la CI (3 contrôles verts) et relues par un autre membre avant
leur fusion.

## 3\. Intégration continue (AQ-7)

|Contrôle|Rôle|Durée mesurée|
|-|-|-|
|Lint (ruff)|Style et erreurs du code|7 s|
|Tests (pytest + PostgreSQL)|Tests unitaires avec une vraie base|28 s|
|Build Docker|Construction de l'image de l'application|26 s|

* Les 3 contrôles tournent en parallèle : verdict en moins de 30 secondes.
* Badge « CI passing » affiché en haut du README.
* La règle de protection de `main` impose une relecture approuvée et la réussite des 3 contrôles
avant tout merge.

## 4\. Démonstration de bout en bout

Chaîne démontrée : `docker compose up -d` → `scripts.create\\\_topics` → `scripts.init\\\_db` →
`streaming.consumer` + `ingestion.producer --anomaly-rate 0.1` → Kafka UI → PostgreSQL →
`streaming.storage --aggregate-day today`.

**Volumes (démonstration du 14 novembre, 12:22 → 12:37)**

|Indicateur|Valeur|
|-|-|
|Messages dans `raw\\\_data`|5 684|
|Messages dans `processed\\\_data`|5 693|
|Échecs d'envoi du producer|0|
|Mesures ayant déclenché au moins une règle|**373**|
|Anomalies stockées dans PostgreSQL|**493** (une mesure peut cumuler plusieurs anomalies)|
|Anomalies injectées détectées|**98,2 %** (111/113, relevé en cours de démonstration)|
|Agrégats quotidiens calculés|5 capteurs|

**Répartition des anomalies par catégorie**

|Catégorie|Anomalies|Mesures|Signification|
|-|-|-|-|
|`qualite\\\_donnees`|330|242|Donnée défectueuse : manquante, négative, hors plage|
|`pollution`|107|75|Épisode de pollution : dépassement des seuils OMS, pic|
|`capteur`|67|67|Capteur défaillant : valeur bloquée|

La plateforme distingue ainsi un problème de donnée, un problème de matériel et un véritable
épisode de pollution, qui n'appellent pas les mêmes actions.

**Point de vigilance — réconciliation des volumes.** Les compteurs Kafka sont légèrement
supérieurs à ceux de la base : `processed\\\_data` compte 9 messages de plus que `raw\\\_data`
(0,16 %), et le topic `anomalies` compte 524 messages pour 493 lignes en base. Deux causes
possibles : la livraison « au moins une fois » de Kafka (doublons écartés en base par la
contrainte d'unicité `uq\\\_anomalies\\\_kafka`), ou un dernier lot non écrit à l'arrêt du consumer.
L'analyse est reportée au Sprint 4.

## 5\. Mesure de la latence Kafka → PostgreSQL (AQ-23)

Mesure sur une fenêtre de 5 minutes, données du simulateur. Résultats complets dans
`docs/metrics/latence-sprint2.csv` et `docs/metrics/latence-sprint2-charge.csv`.

|Scénario|Débit mesuré|Messages|p50|p95|Max|
|-|-|-|-|-|-|
|Faible charge|\~1,2 msg/s|361|1 085 ms|3 078 ms|3 088 ms|
|Forte charge|\~70 msg/s|21 143|346 ms|730 ms|3 081 ms|

|Étape (p50)|Faible charge|Forte charge|
|-|-|-|
|Kafka → traitement|69 ms|77 ms|
|Traitement → base|1 019 ms|267 ms|

**Analyse :**

* Le traitement (validation, fenêtres glissantes, règles) reste rapide sous la charge : 69 à 77 ms en médiane.
* 95 % de la latence vient de l'écriture par lots (50 messages ou délai maximal d'attente).
* À faible débit, un lot de 50 mettrait \~40 s à se remplir : c'est le délai maximal qui déclenche
l'écriture, d'où une latence de 1 à 3 s.
* À 70 msg/s, un lot se remplit en \~0,7 s : la latence baisse (p95 −76 %), sans aucune perte
sur \~23 000 messages.

## 6\. Rétrospective

|Ce qui a bien fonctionné|Ce qui est à améliorer|Actions|
|-|-|-|
|Ordre de fusion respecté : AQ-7 en premier, AQ-18 en dernier|Un membre (Dris) cumulait 2 tickets et 3 relectures : goulot d'étranglement sur AQ-13|Répartir les relectures en tenant compte de la charge de chacun, dès la planification|
|Toutes les PR vérifiées par la CI avant fusion|Tickets passés à « Terminé » avant la fusion de leur PR|Définition de terminé : « PR fusionnée dans main » ; automatisation Jira « PR fusionnée → Terminé »|
|Démo de bout en bout fonctionnelle, avec 98,2 % de détection|Environnements locaux pas à jour (bibliothèques manquantes, Docker non démarré)|Après chaque `git pull` : `pip install -r requirements.txt` ; check-list de démarrage dans le README|
|Mesures de performance chiffrées (latence, détection)|Chiffres relevés pendant que la démo tournait encore|Arrêter producer et consumer avant toute mesure ou requête de bilan|

## 7\. Décisions

1. **PostgreSQL est la source de référence** pour les volumes et les anomalies : la contrainte
d'unicité garantit l'absence de doublons, même en cas de relecture d'un message Kafka.
2. Le paramétrage par lots du consumer (50 messages ou délai maximal) est conservé : une latence
p95 inférieure à 3,1 s est suffisante pour la surveillance de la qualité de l'air.
3. Une **release GitHub `v0.1.0`** marque la fin des Sprints 1 et 2 (ticket AQ-26).
4. Les résultats de mesure (CSV) sont versionnés dans `docs/metrics/` pour être repris dans le rapport.
5. Dans le Sprint 3, chaque membre écrit sa logique sous forme de fonctions Python testables
sans Airflow, branchées dans le DAG une fois AQ-25 fusionné.

## 8\. Planification du Sprint 3 (16 au 22 novembre 2026)

**Objectif :** orchestrer le pipeline quotidien avec Airflow : extraction, contrôles qualité,
agrégats, entraînement et déploiement conditionnel du modèle, déclenchés par des sensors.

|Ticket|Description|Responsable|Points|Priorité|
|-|-|-|-|-|
|AQ-25|Airflow dans le docker-compose (LocalExecutor, métadonnées dans PostgreSQL)|Zakaria|5|Très haute|
|AQ-26|Release v0.1.0 + compte rendu du Sprint 2|Zakaria|1|Basse|
|AQ-27|DAG quotidien : squelette + tâche Extract (mesures de la veille)|Abderrahim|3|Haute|
|AQ-28|Branchement « deploy if better » (BranchPythonOperator)|Abderrahim|3|Moyenne|
|AQ-29|Validate Quality : complétude, fraîcheur, volume + historique des contrôles|Dris|3|Moyenne|
|AQ-31|Aggregate : batch quotidien `aggregate\\\_day` dans le DAG|Dris|2|Moyenne|
|AQ-32|Train + Evaluate : modèle de référence scikit-learn, métriques enregistrées|Aziz|5|Haute|
|AQ-33|SqlSensor : attendre l'arrivée de nouvelles données|Ismail|2|Moyenne|
|AQ-34|Heartbeat du consumer + sensor « fin du traitement streaming »|Ismail|3|Moyenne|
||**Total**||**27**||

**Ordre des dépendances :** AQ-25 → AQ-27 → (AQ-29, AQ-31, AQ-33, AQ-34, AQ-32 en parallèle)
→ AQ-28. AQ-26 est indépendant.

**Vélocité de référence :** 22 points (Sprint 1), 29 points (Sprint 2).

## 9\. Prochaine réunion

Dimanche 22 novembre 2026 : revue du Sprint 3 (démonstration du DAG Airflow quotidien).

*Rédigé par Zakaria Daoudi*

