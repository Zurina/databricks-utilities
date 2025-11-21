# Can be put in a Notebook

from pyspark.sql import functions as F

# Get drift table
df = spark.table(
    "my_databricks.default.airbnb_pricer_inferencelogs_bigler1205_drift_metrics"
)

# Find the row with the latest window.end
latest_window_end = df.agg(
    F.max("window.end")
).collect()[0][0]

# Get latest windows metrics
latest_drift_metrics = df.filter(
    df["window.end"] == latest_window_end
)

# display(latest_drift_metrics)

# Set drift thresholds
js_threshold = 0.1
psi_threshold = 0.25

# Calculate the total average of js_distance and population_stability_index
avg_metrics = latest_drift_metrics.agg(
    F.avg("js_distance").alias("avg_js_distance"),
    F.avg("population_stability_index").alias("avg_population_stability_index")
)

# display(avg_metrics)

# Determine retraining
drift_exceeds_threshold = avg_metrics.filter(
    (F.col("avg_js_distance") > js_threshold) | 
    (F.col("avg_population_stability_index") > psi_threshold)
)
if drift_exceeds_threshold.count() > 0:
    print("Drift detected! Initiating retraining pipeline...")
else:
    print("No significant drift detected. No retraining needed.")