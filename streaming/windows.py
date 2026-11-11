"""Fenêtres glissantes sur les mesures de particules (AQ-20).

Pour chaque capteur, on garde en mémoire les mesures des 15 dernières minutes
et on calcule, à chaque nouveau message :
    - la moyenne sur 5 minutes et sur 15 minutes,
    - le maximum et le nombre de valeurs sur 15 minutes,
pour PM2.5 et PM10.

Le temps utilisé est celui de la MESURE (champ timestamp du message, « event
time »), pas l'heure de l'ordinateur : le calcul reste juste quand on rejoue
des données anciennes (OpenAQ) ou quand des messages arrivent en retard.

Utilisation :
    windows = SlidingWindows()
    avant = windows.stats("CAP_001")                       # état AVANT le message
    apres = windows.update("CAP_001", horodatage, mesures)  # ajoute puis recalcule
    apres["pm2_5"]["mean_5min"]
"""
from collections import defaultdict, deque
from datetime import datetime, timedelta
from math import isfinite

DEFAULT_WINDOWS_MIN = (5, 15)
DEFAULT_PARAMS = ("pm2_5", "pm10")


def parse_timestamp(value):
    """'2026-11-09T10:00:00Z' -> datetime avec fuseau UTC."""
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class SlidingWindows:
    """Fenêtres glissantes par capteur, basées sur l'heure des mesures."""

    def __init__(self, windows_min=DEFAULT_WINDOWS_MIN, params=DEFAULT_PARAMS):
        self.windows_min = sorted(windows_min)
        self.params = tuple(params)
        self.span = timedelta(minutes=self.windows_min[-1])   # la plus grande fenêtre
        self._points = defaultdict(deque)   # capteur -> deque[(horodatage, {param: valeur})]
        self._latest = {}                   # capteur -> horodatage le plus récent vu

    def update(self, sensor_id, timestamp, measurements):
        """Ajoute une mesure, supprime les points trop anciens et renvoie les statistiques."""
        if isinstance(timestamp, str):
            timestamp = parse_timestamp(timestamp)
        latest = max(timestamp, self._latest.get(sensor_id, timestamp))
        cutoff = latest - self.span
        # Purger aussi les points retardés qui ne sont pas en tête.
        points = deque((ts, m) for ts, m in self._points[sensor_id] if ts > cutoff)
        if timestamp > cutoff:
            # Concentrations numériques, finies et positives ou nulles uniquement.
            values = {}
            for param in self.params:
                value = measurements.get(param)
                valid = (
                    isinstance(value, (int, float))
                    and not isinstance(value, bool)
                    and value >= 0
                    and (not isinstance(value, float) or isfinite(value))
                )
                values[param] = value if valid else None
            points.append((timestamp, values))
        self._points[sensor_id] = points
        self._latest[sensor_id] = latest
        return self.stats(sensor_id)

    def stats(self, sensor_id):
        """Statistiques actuelles d'un capteur (valeurs None si la fenêtre est vide)."""
        points = self._points.get(sensor_id, ())
        latest = self._latest.get(sensor_id)
        biggest = self.windows_min[-1]
        result = {}
        for param in self.params:
            entry = {}
            for minutes in self.windows_min:
                values = self._values(points, latest, minutes, param)
                mean = round(sum(values) / len(values), 2) if values else None
                entry[f"mean_{minutes}min"] = mean
            values = self._values(points, latest, biggest, param)
            entry[f"max_{biggest}min"] = max(values) if values else None
            entry[f"count_{biggest}min"] = len(values)
            result[param] = entry
        return result

    @staticmethod
    def _values(points, latest, minutes, param):
        """Valeurs non nulles de `param` dans la fenêtre ]latest - minutes ; latest]."""
        if latest is None:
            return []
        start = latest - timedelta(minutes=minutes)
        return [m[param] for ts, m in points if ts > start and m[param] is not None]
