"""Affiche et exporte le rapport qualité d'une période, jour par jour (AQ-43).

Depuis la racine du dépôt, après python -m scripts.init_db :
    python -m scripts.quality_report --from 2026-11-01 --to 2026-11-15
L'intervalle est inclusif, en UTC. L'export est écrit dans docs/metrics/,
sauf si --csv indique un autre chemin.
"""
import argparse
import csv
import os
from contextlib import closing
from datetime import date, timedelta
from pathlib import Path

from common.config import ROOT_DIR
from common.db import get_connection

VIEW_COLUMNS = (
    "day", "measurements", "valid_measurements", "completeness", "sensors", "sources",
    "anomalies", "checks_passed", "failed_checks",
)
CSV_COLUMNS = (*VIEW_COLUMNS, "status")


def period_report(conn, start, end):
    """Donne une ligne par jour UTC, même si aucune mesure n'est enregistrée."""
    if start > end:
        raise ValueError("La date de début doit précéder ou égaler la date de fin.")

    with conn.cursor() as cur:
        cur.execute(
            "SELECT day, measurements, valid_measurements, completeness, sensors, sources, "
            "anomalies, checks_passed, failed_checks "
            "FROM v_quality_period WHERE day BETWEEN %s AND %s",
            (start, end),
        )
        rows = {row[0]: dict(zip(VIEW_COLUMNS, row, strict=True)) for row in cur.fetchall()}

    report = []
    for offset in range((end - start).days + 1):
        day = start + timedelta(days=offset)
        row = rows.get(day)
        if row is None:
            row = dict.fromkeys(VIEW_COLUMNS)
            row.update(day=day, measurements=0, valid_measurements=0, sensors=0, anomalies=0)
        if row["measurements"] == 0:
            row["status"] = "AUCUNE DONNÉE"
        else:
            row["status"] = check_status(row["checks_passed"])
        report.append(row)
    return report


def check_status(passed):
    if passed is None:
        return "non contrôlée"
    return "OK" if passed else "ÉCHEC"


def summary(report):
    """Résume le rapport, y compris les échecs constatés sur des journées vides."""
    measurements = sum(row["measurements"] for row in report)
    valid = sum(row["valid_measurements"] for row in report)
    days_with_data = sum(row["measurements"] > 0 for row in report)
    return {
        "days": len(report),
        "days_with_data": days_with_data,
        "days_missing": len(report) - days_with_data,
        "days_ok": sum(row["checks_passed"] is True for row in report),
        "days_failed": sum(row["checks_passed"] is False for row in report),
        "measurements": measurements,
        "completeness": valid / measurements if measurements else None,
        "anomalies": sum(row["anomalies"] for row in report),
    }


def format_percent(value):
    return "—" if value is None else f"{value:.1%}"


def print_report(report):
    print(f"{'Jour UTC':<12} {'Mesures':>8} {'Complétude':>12} {'Anomalies':>9} "
          f"{'État des données':<18} {'Contrôles':<14} Échecs")
    for row in report:
        data_status = "AUCUNE DONNÉE" if row["measurements"] == 0 else "Données présentes"
        print(f"{row['day'].isoformat():<12} {row['measurements']:>8} "
              f"{format_percent(row['completeness']):>12} {row['anomalies']:>9} "
              f"{data_status:<18} {check_status(row['checks_passed']):<14} "
              f"{row['failed_checks'] or ''}")
    stats = summary(report)
    print(f"\n{stats['days']} jours | avec données : {stats['days_with_data']} | "
          f"sans données : {stats['days_missing']} | contrôles OK : {stats['days_ok']} | "
          f"en échec : {stats['days_failed']}")
    print(f"{stats['measurements']} mesures | complétude globale "
          f"{format_percent(stats['completeness'])} | {stats['anomalies']} anomalies")


def write_csv(report, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(report)


def parse_date(value):
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        raise argparse.ArgumentTypeError("utilisez le format AAAA-MM-JJ") from None
    if parsed.isoformat() != value:
        raise argparse.ArgumentTypeError("utilisez le format AAAA-MM-JJ")
    return parsed


def main(argv=None):
    parser = argparse.ArgumentParser(description="Rapport qualité d'une période UTC")
    start_default = os.getenv("BACKFILL_START_DATE") or None
    end_default = os.getenv("BACKFILL_END_DATE") or None
    parser.add_argument("--from", dest="start", type=parse_date, default=start_default,
                        required=start_default is None)
    parser.add_argument("--to", dest="end", type=parse_date, default=end_default,
                        required=end_default is None)
    parser.add_argument(
        "--csv", type=Path, help="chemin de l'export (par défaut dans docs/metrics/)"
    )
    args = parser.parse_args(argv)
    if args.start > args.end:
        parser.error("--from doit précéder ou égaler --to")

    with closing(get_connection()) as conn:
        report = period_report(conn, args.start, args.end)
    print_report(report)
    output = args.csv or ROOT_DIR / "docs" / "metrics" / f"qualite-{args.start}-{args.end}.csv"
    write_csv(report, output)
    print(f"Enregistré dans {output}")


if __name__ == "__main__":
    main()
