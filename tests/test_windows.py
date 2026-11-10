"""Tests des fenêtres glissantes (AQ-20)."""
from datetime import datetime, timedelta, timezone

from streaming.windows import SlidingWindows

T0 = datetime(2026, 11, 9, 10, 0, tzinfo=timezone.utc)


def mesure(pm25, pm10=None):
    return {"pm2_5": pm25, "pm10": pm10}


def test_fenetre_vide():
    stats = SlidingWindows().stats("CAP_001")
    assert stats["pm2_5"] == {"mean_5min": None, "mean_15min": None,
                              "max_15min": None, "count_15min": 0}


def test_exemple_chiffre():
    """Mesures à 10:00, 10:04, 10:08, 10:12 (PM2.5 = 10, 20, 30, 40)."""
    w = SlidingWindows()
    for minutes, value in [(0, 10), (4, 20), (8, 30), (12, 40)]:
        stats = w.update("CAP_001", T0 + timedelta(minutes=minutes), mesure(value))
    # À 10:12 : fenêtre 5 min = ]10:07 ; 10:12] -> 30 et 40 ; 15 min -> les 4 valeurs
    assert stats["pm2_5"]["mean_5min"] == 35.0
    assert stats["pm2_5"]["mean_15min"] == 25.0
    assert stats["pm2_5"]["max_15min"] == 40
    assert stats["pm2_5"]["count_15min"] == 4


def test_points_anciens_supprimes():
    w = SlidingWindows()
    w.update("CAP_001", T0, mesure(100))
    stats = w.update("CAP_001", T0 + timedelta(minutes=20), mesure(10))
    assert stats["pm2_5"]["mean_15min"] == 10.0
    assert stats["pm2_5"]["count_15min"] == 1


def test_valeurs_none_ignorees():
    w = SlidingWindows()
    w.update("CAP_001", T0, mesure(10, None))
    stats = w.update("CAP_001", T0 + timedelta(minutes=1), mesure(None, 30))
    assert stats["pm2_5"]["mean_5min"] == 10.0
    assert stats["pm10"]["mean_5min"] == 30.0


def test_capteurs_independants():
    w = SlidingWindows()
    w.update("CAP_001", T0, mesure(10))
    stats = w.update("CAP_002", T0, mesure(50))
    assert stats["pm2_5"]["mean_5min"] == 50.0
    assert w.stats("CAP_001")["pm2_5"]["mean_5min"] == 10.0


def test_horodatage_texte_accepte():
    stats = SlidingWindows().update("CAP_001", "2026-11-09T10:00:00Z", mesure(12.5))
    assert stats["pm2_5"]["mean_15min"] == 12.5
