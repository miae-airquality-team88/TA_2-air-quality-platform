# Compte rendu — Revue du Sprint 1 et planification du Sprint 2

|||
|-|-|
|**Date**|Samedi 8 novembre 2026|
|**Type**|Revue de sprint + rétrospective + planification|
|**Animateur (Scrum Master)**|Zakaria Daoudi|
|**Participants**|Zakaria Daoudi, Ismail Bouabid, Dris Alamass, Abderrahim Elharragui, Aziz Agourram |
|**Sprint concerné**|AQ Sprint 1 — 2 au 9 novembre 2026|

\---

## 1\. Objectif du Sprint 1

> Infrastructure Kafka + PostgreSQL dockerisée, contrat de données défini et mesures des capteurs
> (simulées et OpenAQ) publiées dans Kafka.

**Résultat : objectif \[atteint / partiellement atteint — à compléter].**

## 2\. Tickets du Sprint 1

|Ticket|Description|Responsable|Statut|
|-|-|-|-|
|AQ-6|Docker-compose : Kafka (KRaft) + PostgreSQL + Kafka UI|Zakaria|Terminé|
|AQ-8|Squelette du projet, CONTRIBUTING, modèle de PR|Zakaria|\[à compléter]|
|AQ-15|Contrat de données (JSON Schema v1.0)|Dris|\[à compléter]|
|AQ-10|Script de création des topics|Dris|\[à compléter]|
|AQ-9|Simulateur de capteurs IoT avec injection d'anomalies|Ismail|\[à compléter]|
|AQ-11|Producer Kafka vers `raw\_data`|Aziz|\[à compléter]|
|AQ-14|(Bonus) Historique OpenAQ de la station 3847229|Aziz|\[à compléter]|
|AQ-16|(Bonus) Rejeu OpenAQ dans `raw\_data`|Abderrahim|\[à compléter]|

## 3\. Démonstration

Chaîne démontrée : `docker compose up -d` → `python -m scripts.create\_topics` →
`python -m ingestion.producer` → vérification dans Kafka UI.

Constat dans Kafka UI (topic `raw\_data`) :

|Partition|Messages|
|-|-|
|0|0|
|1|40|
|2|60|
|**Total**|**100** (5 capteurs × 20 envois)|

**Analyse :** la répartition est due à la clé des messages (`sensor\_id`). Kafka place tous les
messages d'une même clé dans la même partition (ce qui garantit leur ordre). Avec seulement
5 clés (`CAP\_001` à `CAP\_005`), le hachage en a placé 2 dans la partition 1 et 3 dans la
partition 2 ; la partition 0 n'a reçu aucun capteur. Ce n'est pas une erreur : la charge
s'équilibrera avec plus de capteurs (`--sensors 20`) ou avec la clé `OPENAQ\_3847229` du rejeu.
Point à mentionner dans le rapport (choix de la clé = compromis entre ordre et répartition).

## 4\. Rétrospective

|Ce qui a bien fonctionné|Ce qui est à améliorer|Actions|
|-|-|-|
|Circuit ticket Jira → branche → PR → relecture opérationnel|\[à compléter]|\[à compléter]|
|Contrat de données partagé : formats cohérents entre les modules|\[à compléter]|\[à compléter]|
|Infrastructure démarrée en une commande|\[à compléter]|\[à compléter]|

## 5\. Décisions

1. Le dépôt GitHub est renommé **`TA\_2-air-quality-platform`** : chacun met à jour son dépôt local
(`git remote set-url origin https://github.com/miae-airquality-team88/TA\_2-air-quality-platform.git`).
2. Le consumer streaming est écrit en **Python** (et non Spark), comme le permet le sujet
(« Spark/Python ») : plus léger pour nos PC et plus simple à tester.
3. Une **intégration continue** (GitHub Actions) est mise en place en début de Sprint 2 : plus
aucune PR ne peut être fusionnée si le lint, les tests ou la construction Docker échouent.
4. Les seuils d'alerte de pollution sont les **cibles intermédiaires 1 de l'OMS (2021)** :
PM2.5 75, PM10 150, NO₂ 120, O₃ 160 µg/m³.

## 6\. Planification du Sprint 2 (9 au 16 novembre 2026)

**Objectif :** Consumer streaming opérationnel : mesures validées, agrégées, anomalies détectées
et stockées dans PostgreSQL. CI GitHub Actions active.

|Ticket|Description|Responsable|Points|
|-|-|-|-|
|AQ-7|CI GitHub Actions : lint + tests + build Docker|Zakaria|3|
|AQ-12|Schéma PostgreSQL : temps réel + agrégats quotidiens|Dris|3|
|AQ-13|Validation des mesures (Pydantic) + tests|Dris|5|
|AQ-18|Consumer Kafka : lecture de `raw\_data` et validation|Ismail|3|
|AQ-19|Publication des mesures nettoyées dans `processed\_data`|Ismail|2|
|AQ-20|Fenêtres glissantes PM2.5/PM10 sur 5 et 15 min|Aziz|5|
|AQ-21|Règles métier : seuils OMS → `anomalies`|Abderrahim|3|
|AQ-22|Écriture du flux dans PostgreSQL (temps réel + agrégats)|Abderrahim|3|
|AQ-23|Mesure de la latence Kafka → PostgreSQL|Zakaria|2|
||**Total**||**29**|

**Ordre de fusion :** AQ-7 → AQ-12 → (AQ-13, AQ-20, AQ-21, AQ-19 en parallèle) → AQ-22 → AQ-18 → AQ-23.

## 7\. Prochaine réunion

Samedi 15 novembre 2026 : revue du Sprint 2 (démonstration de bout en bout : simulateur →
Kafka → consumer → PostgreSQL, avec rapport de latence).

