-- Rapport qualité AQ-43. Le nom place la vue après init.sql et
-- 010_quality_checks.sql dans l'ordre d'initialisation du projet.
CREATE OR REPLACE VIEW v_quality_period AS
WITH mesures AS (
    SELECT (measured_at AT TIME ZONE 'UTC')::date AS day,
           COUNT(*) AS measurements,
           COUNT(*) FILTER (WHERE is_valid) AS valid_measurements,
           COUNT(DISTINCT sensor_id) AS sensors,
           STRING_AGG(DISTINCT source, ', ' ORDER BY source) AS sources
    FROM measurements
    GROUP BY 1
),
anomalies_jour AS (
    SELECT (measured_at AT TIME ZONE 'UTC')::date AS day,
           COUNT(*) AS anomalies
    FROM anomalies
    WHERE measured_at IS NOT NULL
    GROUP BY 1
),
derniers_controles AS (
    SELECT DISTINCT ON (day, check_name) day, check_name, passed
    FROM quality_checks
    ORDER BY day, check_name, checked_at DESC, id DESC
),
controles AS (
    SELECT day,
           BOOL_AND(passed) AS checks_passed,
           STRING_AGG(check_name, ', ' ORDER BY check_name) FILTER (WHERE NOT passed) AS failed_checks
    FROM derniers_controles
    GROUP BY day
)
SELECT day,
       COALESCE(m.measurements, 0) AS measurements,
       COALESCE(m.valid_measurements, 0) AS valid_measurements,
       ROUND(m.valid_measurements::numeric / m.measurements, 4) AS completeness,
       COALESCE(m.sensors, 0) AS sensors,
       m.sources,
       COALESCE(a.anomalies, 0) AS anomalies,
       c.checks_passed,
       c.failed_checks
FROM mesures m
FULL JOIN anomalies_jour a USING (day)
FULL JOIN controles c USING (day);
