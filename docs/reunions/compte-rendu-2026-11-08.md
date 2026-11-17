# Compte rendu — Revue du Sprint 1 et planification du Sprint 2

|||
|-|-|
|**Date**|Dimanche 8 novembre 2026|
|**Type**|Revue de sprint + rétrospective + planification|
|**Animateur (Scrum Master)**|Zakaria Daoudi|
|**Participants**|Zakaria Daoudi, Ismail Bouabid, Dris Alamass, Abderrahim Elharragui, Aziz Agourram|
|**Sprint concerné**|AQ Sprint 1 — 2 au 9 novembre 2026|
|**Ticket Jira**|AQ-24|

\---

## 1\. Objectif du Sprint 1

> Infrastructure Kafka + PostgreSQL dockerisée, contrat de données défini et mesures des capteurs
> (simulées et OpenAQ) publiées dans Kafka.

**Résultat : objectif atteint — 22 points terminés sur 22 (100 %).**

## 2\. Tickets du Sprint 1

|Ticket|Description|Responsable|Points|PR|Statut|
|-|-|-|-|-|-|
|AQ-6|Docker-compose : Kafka (KRaft) + PostgreSQL + Kafka UI|Zakaria|3|#9|Terminé|
|AQ-8|Squelette du projet, CONTRIBUTING, modèle de PR|Zakaria|1|#11|Terminé|
|AQ-15|Contrat de données (JSON Schema v1.0)|Dris|2|#14|Terminé|
|AQ-10|Script de création des topics (idempotent)|Dris|2|#19|Terminé|
|AQ-9|Simulateur de capteurs IoT avec injection d'anomalies|Ismail|5|#15, #16|Terminé|
|AQ-11|Producer Kafka vers `raw\\\\\\\_data` (clé `sensor\\\\\\\_id`, `acks=all`)|Aziz|3|#20|Terminé|
|AQ-14|Historique OpenAQ de la station 3847229|Aziz|3|#17|Terminé|
|AQ-16|Rejeu OpenAQ dans `raw\\\\\\\_data`|Abderrahim|3|#21|Terminé|
|AQ-24|Compte rendu de la réunion n°4 (ce document)|Zakaria|0|#22|Terminé|
||**Total**||**22 / 22**|||

Les tickets d'ingestion (AQ-9, AQ-10, AQ-11, AQ-14, AQ-16) ont été fusionnés dans `main` entre
le 4 et le 8 novembre, chacun par une PR relue par un autre membre de l'équipe.

**Données OpenAQ récupérées (AQ-14) :** station ARC Air (ID 3847229), octobre 2026,
5 paramètres (PM1, PM2.5, humidité, température, UM003) × 744 mesures horaires = **3 720 mesures**,
sans heure manquante. La station ne mesure pas le PM10.

## 3\. Démonstration

Chaîne démontrée : `docker compose up -d` → `python -m scripts.create\\\\\\\_topics` →
`python -m ingestion.producer` → vérification dans Kafka UI.

Constat dans Kafka UI (topic `raw\\\\\\\_data`) :

|Partition|Messages|
|-|-|
|0|0|
|1|40|
|2|60|
|**Total**|**100** (5 capteurs × 20 envois)|

**Analyse :** la répartition est due à la clé des messages (`sensor\\\\\\\_id`). Kafka place tous les
messages d'une même clé dans la même partition (ce qui garantit leur ordre). Avec seulement
5 clés (`CAP\\\\\\\_001` à `CAP\\\\\\\_005`), le hachage en a placé 2 dans la partition 1 et 3 dans la
partition 2 ; la partition 0 n'a reçu aucun capteur. Ce n'est pas une erreur : la charge
s'équilibrera avec plus de capteurs (`--sensors 20`) ou avec la clé `OPENAQ\\\\\\\_3847229` du rejeu.
Le choix de la clé est un compromis entre ordre des messages et répartition de la charge.

## 4\. Rétrospective

|Ce qui a bien fonctionné|Ce qui est à améliorer|Actions|
|-|-|-|
|Circuit ticket Jira → branche → PR → relecture opérationnel|Premières PR créées depuis l'interface web, sans clé Jira|Toute modification passe par une branche `type/AQ-xx-…` (CONTRIBUTING, AQ-8)|
|Contrat de données partagé : formats cohérents entre les modules|Problèmes de virtualisation Docker sous Windows chez certains membres|Vérification de WSL2 et de Docker Desktop par chacun avant le Sprint 2|
|Infrastructure démarrée en une commande|PR ouvertes tard dans le sprint|PR ouverte dès que les critères d'acceptation sont remplis, relecture sous 24 h|

## 5\. Décisions

1. Le dépôt GitHub est renommé **`TA\\\\\\\_2-air-quality-platform`** : chacun met à jour son dépôt local
(`git remote set-url origin https://github.com/miae-airquality-team88/TA\\\\\\\_2-air-quality-platform.git`).
2. Le consumer streaming est écrit en **Python** (et non Spark), comme le permet le sujet
(« Spark/Python ») : plus léger pour nos PC et plus simple à tester.
3. Une **intégration continue** (GitHub Actions) est mise en place en début de Sprint 2 : plus
aucune PR ne peut être fusionnée si le lint, les tests ou la construction Docker échouent.
4. Les seuils d'alerte de pollution sont les **cibles intermédiaires 1 de l'OMS (2021)** :
PM2.5 75, PM10 150, NO₂ 120 µg/m³ (moyennes sur 24 h), O₃ 160 µg/m³ (moyenne sur 8 h).
Pour la station OpenAQ 3847229, sans PM10, l'alerte porte sur le PM2.5.

## 6\. Planification du Sprint 2 (9 au 16 novembre 2026)

**Objectif :** consumer streaming opérationnel : mesures validées, agrégées, anomalies détectées
et stockées dans PostgreSQL. CI GitHub Actions active.

|Ticket|Description|Responsable|Points|
|-|-|-|-|
|AQ-7|CI GitHub Actions : lint + tests + build Docker|Zakaria|3|
|AQ-12|Schéma PostgreSQL : temps réel + agrégats quotidiens|Dris|3|
|AQ-13|Validation des mesures (Pydantic) + tests|Dris|5|
|AQ-18|Consumer Kafka : lecture de `raw\\\\\\\_data` et validation|Ismail|3|
|AQ-19|Publication des mesures nettoyées dans `processed\\\\\\\_data`|Ismail|2|
|AQ-20|Fenêtres glissantes PM2.5/PM10 sur 5 et 15 min|Aziz|5|
|AQ-21|Règles métier : seuils OMS → `anomalies`|Abderrahim|3|
|AQ-22|Écriture du flux dans PostgreSQL (temps réel + agrégats)|Abderrahim|3|
|AQ-23|Mesure de la latence Kafka → PostgreSQL|Zakaria|2|
||**Total**||**29**|

**Vélocité du Sprint 1 :** 22 points. Le Sprint 2 en prévoit 29 : si l'avancement le nécessite
en milieu de sprint, AQ-23 sera reporté au Sprint 3.

**Ordre de fusion :** AQ-7 → AQ-12 → (AQ-13, AQ-20, AQ-21, AQ-19 en parallèle) → AQ-22 → AQ-18 → AQ-23.

## 7\. Prochaine réunion

Dimanche 15 novembre 2026 : revue du Sprint 2 (démonstration de bout en bout : simulateur →
Kafka → consumer → PostgreSQL, avec rapport de latence).

*Rédigé par Zakaria Daoudi*

