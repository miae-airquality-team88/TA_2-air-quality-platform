# Compte rendu — Réunion n°6 : revue du Sprint 3 et planification du Sprint 4

|||
|-|-|
|**Date**|Dimanche 22 novembre 2026|
|**Équipe**|TA\_2 — plateforme de détection d'anomalies de la qualité de l'air|
|**Participants**|Zakaria Daoudi (Scrum Master / DevOps), Ismail Bouabid, Driss Alamass, Aziz Agourram, Abderrahim Elharragui|
|**Rédacteur**|Zakaria Daoudi|
|**Ticket Jira**|AQ-35|

## Ordre du jour

1. Bilan du Sprint 3 (16 → 22 novembre) : Airflow
2. Démonstration du DAG quotidien
3. Incidents rencontrés et solutions
4. Rétrospective
5. Décisions
6. Planification du Sprint 4 (23 → 29 novembre)

\---

## 1\. Bilan du Sprint 3

**Objectif du sprint :** orchestrer le pipeline quotidien avec Airflow : Extract → Validate Quality → Train → Evaluate → Branch (deploy if better / end), avec des sensors qui attendent les nouvelles données et la fin du streaming.

**Résultat : objectif atteint.** Tous les tickets engagés sont terminés et fusionnés sur `main` après relecture et passage des 4 contrôles de la CI (Lint, Tests, Build Docker, DAGs Airflow).

|Ticket|Réalisation|Responsable|Livraison|
|-|-|-|-|
|AQ-25|Airflow 2.10.5 dans le docker-compose (LocalExecutor, base `airflow` dans PostgreSQL, profil `airflow`) + contrôle CI « DAGs Airflow »|Zakaria|Fusionné sur `main`|
|AQ-26|Comptes rendus du Sprint 2 + release `v0.1.0`|Zakaria| tag `v0.1.0`|
|AQ-33|`SqlSensor` : attente de nouvelles données pour la journée|Ismail|Fusionné sur `main`|
|AQ-34|Signal d'activité du consumer (`pipeline\_heartbeats`, retard Kafka) + sensor de fin du streaming|Ismail|Fusionné sur `main`|
|AQ-29|Tâche Validate Quality : contrôles volume, complétude, fraîcheur, enregistrés dans `quality\_checks`|Driss|Fusionné sur `main`|
|AQ-31|Tâche d'agrégation quotidienne (`daily\_aggregates`) avec vérification des volumes|Driss|Fusionné sur `main`|
|AQ-32|Modèle de référence : forêt aléatoire anomalie / normal, évalué par F1, précision, rappel, ROC AUC|Aziz|Fusionné sur `main`|
|AQ-28|Déploiement conditionnel « deploy if better » (`BranchPythonOperator`)|Abderrahim|Fusionné sur `main`|
|AQ-27|DAG complet `air\_quality\_daily` (11 tâches, `@daily`, `catchup=False`, `max\_active\_runs=1`)|Abderrahim (en binôme avec Zakaria)|Fusionné sur `main` le 22/11|

**Suivi de la vélocité :** Sprint 1 : 22 / 22 points · Sprint 2 : 29 / 29 points · Sprint 3 : 100 % des tickets engagés terminés.

\---

## 2\. Démonstration

### 2.1 Infrastructure et streaming

* Kafka (KRaft), PostgreSQL 16, Kafka UI et Airflow démarrés avec `docker compose`.
* Le producer a publié 100 messages ; le consumer les a lus, a détecté 9 anomalies, et son retard Kafka est revenu à 0.

### 2.2 Exécution planifiée du 21 novembre (sans intervention humaine)

Au démarrage d'Airflow, le planificateur a **rattrapé automatiquement** la journée du 21 novembre. Les 11 tâches se sont exécutées de bout en bout.

|Contrôle qualité|Valeur|Seuil|Résultat|
|-|-|-|-|
|Volume|300 mesures|≥ 100|✅|
|Complétude|293 / 300 valides (97,7 %)|≥ 80 %|✅|
|Fraîcheur|dernière mesure il y a 0,02 h|≤ 200 h|✅|

**Évaluation du modèle candidat** (jeu de test de 75 lignes) :

|F1|Précision|Rappel|Accuracy|ROC AUC|
|-|-|-|-|-|
|0,80|1,00|0,667|0,987|0,755|

Lecture : aucune fausse alerte (précision 1), mais une anomalie sur trois est manquée (rappel 0,667). L'accuracy de 98,7 % est trompeuse, car les anomalies ne représentent que 4 % des mesures : la décision repose donc sur le **F1**.

**Décision du pipeline :** `f1 0.8 <= 0.8552 (production) : on garde le modèle actuel`. La branche `skip\_deploy` est choisie et `deploy\_model` est sauté.

### 2.3 Historique des décisions de déploiement

|Exécution|F1 du candidat|Modèle en production|Décision|
|-|-|-|-|
|14/11, 1ʳᵉ exécution manuelle|0,8552|aucun|Déployé|
|14/11, 2ᵉ exécution manuelle|non meilleur|0,8552|Conservé (`skip\_deploy`)|
|21/11, exécution planifiée|0,80|0,8552|Conservé (`skip\_deploy`)|

Un nouvel entraînement n'est pas automatiquement une amélioration : le pipeline ne déploie que si le candidat fait mieux.

### 2.4 Échec volontaire : protection contre des données insuffisantes

* Le seuil de volume a été porté à 1000 mesures (`QUALITY\_MIN\_ROWS=1000` dans `.env`, sans modifier le code), puis le DAG a été relancé pour le 21 novembre (300 mesures).
* `validate\_quality` a échoué avec `QualityCheckError : Contrôles qualité en échec pour le 2026-11-21 : volume`.
* Les tâches suivantes (agrégation, entraînement, évaluation, déploiement) sont passées en `upstream\_failed` : **aucun modèle n'est entraîné sur des données insuffisantes**, et le modèle en production reste intact.
* Les trois contrôles restent tracés dans `quality\_checks`, y compris l'échec : `volume | 300 | 1000 | passed = f | 300 mesures (minimum 1000)`.

\---

## 3\. Incidents et solutions

|Incident|Cause|Solution|
|-|-|-|
|Le conteneur Airflow redémarrait en boucle (« database airflow does not exist »)|Le point d'entrée de l'image vérifie la base avant que notre script ne la crée|Variable `CONNECTION\_CHECK\_MAX\_COUNT=0` ; vérifié en repartant d'une base supprimée|
|La release `v0.1.0` devait exclure AQ-25, déjà fusionné|Ordre de fusion|Tag annoté placé sur le commit précédent (`df00b44`)|
|Une exécution planifiée sans données bloquait l'exécution manuelle|`max\_active\_runs=1`|Exécution marquée en échec dans Browse → DAG Runs|
|`wait\_streaming\_done` bloqué avec un retard de 48 690 messages alors que Kafka indiquait 0|`compute\_lag` renvoie la taille du topic quand le consumer n'a encore lu aucun message|Contournement : publier quelques messages pendant que le consumer tourne. **Correction prévue au Sprint 4**|
|Panne du poste d'Abderrahim pendant AQ-27|Matériel|Travail en binôme Abderrahim / Zakaria, co-auteur indiqué dans le commit (`Co-authored-by`) ; Abderrahim reste l'assigné|
|Le contrôle de fraîcheur ne pouvait pas servir à l'échec volontaire|Il est « non applicable » pour une journée ancienne (rejeu, backfill)|Échec démontré par le seuil de volume, configurable dans `.env`|

\---

## 4\. Rétrospective

**Ce qui a bien fonctionné**

* Le pipeline complet tourne de bout en bout, y compris en exécution planifiée, sans intervention.
* La CI à 4 contrôles obligatoires et la relecture croisée ont évité toute régression sur `main`.
* L'ordre de fusion fondé sur les dépendances (AQ-25 en premier, le DAG AQ-27 en dernier) a été respecté.

**Ce qu'il faut améliorer**

* Les cas limites (consumer qui n'a encore rien lu, journée ancienne) ont été découverts pendant les essais, pas par les tests.
* La démo dépend de données préparées la veille sur un seul poste.
* Une panne de poste peut bloquer un ticket : il faut que chacun puisse lancer Airflow sur sa machine.

**Actions**

* Ajouter un test pytest pour chaque cas limite découvert (dès le Sprint 4).
* Chaque membre vérifie qu'Airflow démarre sur son poste avant de commencer son ticket.
* Le générateur d'historique du Sprint 4 (AQ-36) supprime la dépendance aux données de la veille.

\---

## 5\. Décisions

1. **Le Sprint 4 termine la Phase 2 (Airflow).** Le serveur MLflow, prévu au départ au Sprint 4, passe au Sprint 5 avec le reste de la Phase 3 (expérimentations, Registry, opérateur personnalisé). Justification : livrer une Phase 2 complète et testée avant d'ouvrir la Phase 3. La date de début de l'Epic AQ-4 est décalée au 30 novembre.
2. **Le bug `compute\_lag` est corrigé dans le cadre d'AQ-39** (DAG de surveillance du consumer), qui dépend directement de la valeur du retard.
3. **Les seuils qualité restent configurables dans `.env`** (`QUALITY\_MIN\_ROWS`, `QUALITY\_MIN\_COMPLETENESS`, `QUALITY\_MAX\_AGE\_HOURS`), sans modification du code.
4. **Release `v0.2.0`** à la fin du Sprint 4, pour marquer la fin de la Phase 2.

\---

## 6\. Planification du Sprint 4 (23 → 29 novembre)

**Objectif :** « Phase 2 terminée : rejeu de l'historique par un DAG de backfill, alertes automatiques en cas d'échec. »

|Ticket|Résumé|Responsable|Points|
|-|-|-|-|
|AQ-35|Compte rendu de la revue du Sprint 3|Zakaria|1|
|AQ-36|Générateur d'historique : 30 jours simulés|Driss|3|
|AQ-37|Historique réel OpenAQ (30 jours) en base|Aziz|3|
|AQ-38|Alertes en cas d'échec (`on\_failure\_callback`) + historique des alertes en base|Ismail|3|
|AQ-39|DAG de surveillance du consumer (+ correction de `compute\_lag`)|Ismail|2|
|AQ-40|Fiche descriptive du modèle (model card)|Aziz|2|
|AQ-41|DAG de backfill : réexécution historique (`catchup=True`, dates de début et de fin)|Abderrahim|5|
|AQ-42|Tests de structure des DAGs dans la CI|Zakaria|3|
|AQ-43|Rapport qualité de la période rejouée|Driss|2|
|AQ-44|Release `v0.2.0` : fin de la Phase 2|Zakaria|1|
||**Total**||**25**|

**Dépendances**

```
AQ-36 (Driss) ──┐
                ├─► AQ-41 backfill (Abderrahim) ─► AQ-43 rapport qualité (Driss)
AQ-37 (Aziz) ───┘
AQ-38 alertes (Ismail) ─► AQ-39 surveillance (Ismail)
AQ-40, AQ-42, AQ-35 : indépendants
Tous les tickets ─► AQ-44 release v0.2.0 (en dernier)
```

**Points d'attention**

* Les données historiques (AQ-36, AQ-37) doivent compter au moins 100 mesures par jour et plus de 80 % de mesures valides, sinon `validate\_quality` rejettera les journées rejouées.
* Rejouer une journée deux fois ne doit pas créer de doublons (idempotence).
* La clé API OpenAQ n'est jamais commitée ; elle est transmise uniquement en message privé.

\---

**Prochaine réunion :** dimanche 29 novembre 2026 — revue du Sprint 4, release `v0.2.0` et planification du Sprint 5.

