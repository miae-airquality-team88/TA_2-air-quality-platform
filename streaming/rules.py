"""Règles métier de détection d'anomalies (AQ-21).

Pour chaque message validé, le détecteur produit une liste d'anomalies
(éventuellement vide), publiées ensuite dans le topic `anomalies`.

Quatre familles de règles :

- qualite_donnees / negative, out_of_range, missing :
      problème de contenu signalé par la validation (AQ-13)
- pollution / seuil_oms :
      valeur > cible intermédiaire 1 de l'OMS (tableau OMS_THRESHOLDS)
- pollution / pic :
      valeur > 3 x la moyenne des 15 dernières minutes du même capteur
- capteur / capteur_bloque :
      mesures exactement identiques au message précédent du même capteur

Gravité : « critique » si la valeur dépasse 2 x le seuil OMS, « alerte » pour
les autres anomalies de pollution et de capteur, « info » pour une mesure absente.

Utilisation :
    detector = AnomalyDetector()
    anomalies = detector.detect(message_nettoye, issues, stats_fenetre_avant)
"""

# Seuils d'alerte : cibles intermédiaires 1 (IT-1) des lignes directrices OMS 2021, en µg/m³
# (moyennes 24 h ; 8 h pour l'ozone). Les valeurs « normales » du simulateur restent en dessous,
# les pics injectés (x5 à x10) les dépassent.
OMS_THRESHOLDS = {"pm2_5": 75.0, "pm10": 150.0, "no2": 120.0, "o3": 160.0}

SPIKE_FACTOR = 3.0        # « pic » = valeur > 3 x moyenne 15 min
SPIKE_MIN_POINTS = 3      # il faut au moins 3 valeurs dans la fenêtre pour juger
SPIKE_PARAMS = ("pm2_5", "pm10")

SEVERITY_BY_QUALITY = {"missing": "info", "negative": "alerte", "out_of_range": "alerte"}


def make_anomaly(message, category, anomaly_type, severity, detail,
                 parameter=None, value=None, threshold=None):
    """Construit une anomalie au format commun du projet."""
    return {
        "sensor_id": message.get("sensor_id"),
        "timestamp": message.get("timestamp"),
        "source": message.get("source"),
        "category": category,
        "anomaly_type": anomaly_type,
        "parameter": parameter,
        "value": value,
        "threshold": threshold,
        "severity": severity,
        "detail": detail,
        "injected_anomaly": message.get("injected_anomaly"),
    }


class AnomalyDetector:
    """Applique les règles métier. Garde en mémoire le dernier message de chaque capteur."""

    def __init__(self):
        self._previous = {}   # capteur -> mesures du message précédent

    def detect(self, message, issues, window_stats):
        """Renvoie la liste des anomalies d'un message.

        message      : message NETTOYÉ (sortie de la validation)
        issues       : problèmes de qualité trouvés par la validation
        window_stats : statistiques des fenêtres AVANT l'ajout de ce message
                       (sinon le pic ferait lui-même monter la moyenne)
        """
        anomalies = []
        anomalies += self._quality(message, issues)
        anomalies += self._oms_thresholds(message)
        anomalies += self._spikes(message, window_stats)
        anomalies += self._stuck(message, issues)
        return anomalies

    @staticmethod
    def _quality(message, issues):
        return [
            make_anomaly(message, "qualite_donnees", issue["kind"],
                         SEVERITY_BY_QUALITY.get(issue["kind"], "alerte"), issue["detail"],
                         parameter=issue["field"], value=issue["value"])
            for issue in issues
            if issue["kind"] != "schema"
        ]

    @staticmethod
    def _oms_thresholds(message):
        anomalies = []
        for param, threshold in OMS_THRESHOLDS.items():
            value = message["measurements"].get(param)
            if value is not None and value > threshold:
                severity = "critique" if value > 2 * threshold else "alerte"
                anomalies.append(make_anomaly(
                    message, "pollution", "seuil_oms", severity,
                    f"{param} = {value} > seuil OMS {threshold}",
                    parameter=param, value=value, threshold=threshold))
        return anomalies

    @staticmethod
    def _spikes(message, window_stats):
        anomalies = []
        for param in SPIKE_PARAMS:
            value = message["measurements"].get(param)
            stats = (window_stats or {}).get(param) or {}
            mean, count = stats.get("mean_15min"), stats.get("count_15min", 0)
            if value is None or not mean or count < SPIKE_MIN_POINTS:
                continue
            limit = round(SPIKE_FACTOR * mean, 2)
            if value > limit:
                anomalies.append(make_anomaly(
                    message, "pollution", "pic", "alerte",
                    f"{param} = {value} > {SPIKE_FACTOR:g} x moyenne 15 min ({mean})",
                    parameter=param, value=value, threshold=limit))
        return anomalies

    def _stuck(self, message, issues):
        sensor_id = message["sensor_id"]
        current = message["measurements"]
        previous = self._previous.get(sensor_id)
        self._previous[sensor_id] = current
        has_values = any(v is not None for v in current.values())
        if previous is not None and has_values and current == previous:
            return [make_anomaly(message, "capteur", "capteur_bloque", "alerte",
                                 "mesures identiques au message précédent")]
        return []
