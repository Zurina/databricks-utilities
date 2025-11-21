WITH latest_window AS (
  SELECT
    MAX(window.start) AS max_window_start
  FROM
    airbnb_pricer_inferencelogs_bigler1205_drift_metrics
  WHERE
    (js_distance IS NOT NULL OR population_stability_index IS NOT NULL)
),
metrics_in_window AS (
  SELECT
    window.start AS window_start,
    window.end AS window_end,
    column_name,
    js_distance,
    population_stability_index
  FROM
    airbnb_pricer_inferencelogs_bigler1205_drift_metrics
  JOIN
    latest_window
  ON
    window.start = latest_window.max_window_start
)
SELECT
  window_start,
  window_end,
  AVG(js_distance) AS avg_js_distance,
  AVG(population_stability_index) AS avg_psi,
  CASE WHEN AVG(js_distance) > 0.1 THEN 'ABOVE_THRESHOLD' ELSE 'BELOW_THRESHOLD' END AS js_distance_status,
  CASE WHEN AVG(population_stability_index) > 0.25 THEN 'ABOVE_THRESHOLD' ELSE 'BELOW_THRESHOLD' END AS psi_status
FROM
  metrics_in_window
GROUP BY
  window_start,
  window_end;