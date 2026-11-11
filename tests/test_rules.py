"""Tests des règles métier (AQ-21)."""
import copy

from streaming.rules import AnomalyDetector

BASE = {
    "sensor_id": "CAP_001",
    "timestamp": "2026-11-09T10:00:00Z",
    "location": {"lat": 30.42, "lon": -9.6},
    "measurements": {"pm2_5": 15.0, "pm10": 30.0, "no2": 35.0, "o3": 70.0,
                     "temperature": 22.0, "humidity": 60.0},
    "status": "active",
    "source": "simulator",
    "injected_anomaly": None,
}


def message(**measurements):
    m = copy.deepcopy(BASE)
    m["measurements"].update(measurements)
    return m


def window(mean_pm25, count=5):
    return {"pm2_5": {"mean_15min": mean_pm25, "count_15min": count},
            "pm10": {"mean_15min": None, "count_15min": 0}}


def types(anomalies):
    return [(a["anomaly_type"], a["parameter"]) for a in anomalies]


def test_message_normal_aucune_anomalie():
    assert AnomalyDetector().detect(message(), [], window(15.0)) == []


def test_seuil_oms_alerte_et_critique():
    d = AnomalyDetector()
    alerte = d.detect(message(pm2_5=100.0), [], {})
    assert ("seuil_oms", "pm2_5") in types(alerte)
    assert alerte[0]["severity"] == "alerte"
    critique = AnomalyDetector().detect(message(pm2_5=200.0), [], {})
    assert critique[0]["severity"] == "critique"
    assert critique[0]["threshold"] == 75.0


def test_pic_par_rapport_a_la_moyenne():
    # 50 > 3 x 15 = 45, mais sous le seuil OMS (75) : seule la règle « pic » se déclenche
    anomalies = AnomalyDetector().detect(message(pm2_5=50.0), [], window(15.0))
    assert types(anomalies) == [("pic", "pm2_5")]
    assert anomalies[0]["threshold"] == 45.0


def test_pas_de_pic_si_fenetre_trop_courte():
    assert AnomalyDetector().detect(message(pm2_5=50.0), [], window(15.0, count=2)) == []


def test_problemes_de_qualite_transformes_en_anomalies():
    issues = [{"field": "pm10", "kind": "negative", "value": -12.0, "detail": "valeur négative"},
              {"field": "no2", "kind": "missing", "value": None, "detail": "mesure absente"}]
    anomalies = AnomalyDetector().detect(message(pm10=None, no2=None), issues, {})
    assert ("negative", "pm10") in types(anomalies)
    assert ("missing", "no2") in types(anomalies)
    severites = {a["anomaly_type"]: a["severity"] for a in anomalies}
    assert severites == {"negative": "alerte", "missing": "info"}


def test_capteur_bloque():
    d = AnomalyDetector()
    assert d.detect(message(), [], {}) == []
    assert types(d.detect(message(), [], {})) == [("capteur_bloque", None)]


def test_capteurs_differents_pas_bloques():
    d = AnomalyDetector()
    d.detect(message(), [], {})
    autre = message()
    autre["sensor_id"] = "CAP_002"
    assert d.detect(autre, [], {}) == []


def test_verite_terrain_recopiee():
    m = message(pm2_5=200.0)
    m["injected_anomaly"] = "spike"
    assert AnomalyDetector().detect(m, [], {})[0]["injected_anomaly"] == "spike"
