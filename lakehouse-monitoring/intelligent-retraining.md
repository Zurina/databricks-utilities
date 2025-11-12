# What’s Possible Today with Lakehouse Monitoring

Lakehouse Monitoring writes metrics to Delta tables in **Unity Catalog** (profile and drift tables) for time series and inference profiles, and auto-generates a **DBSQL dashboard** you can customize and alert on.

You can create **Databricks SQL alerts** on drift/profile metrics and send notifications/webhooks — this is the **recommended alerting mechanism** over the monitoring tables.

> ⚠️ Alerts don’t natively trigger Databricks Jobs.  
> The supported option is to notify via email/Slack or a webhook, and then have that webhook call your automation.  
> Direct “alert → job” wiring isn’t on the current roadmap.

---

## Pattern 1 — Event-Driven Retraining Fully Inside Databricks (No External API)

**This is the preferred approach** for most use cases.

### Steps

1. **Attach a Table Update Trigger**  
   Attach it to the drift metrics table that your inference classification monitor writes.  
   This makes your workflow event-driven — your job runs only when new drift metrics are materialized, not on a polling schedule.

2. **Task A — Evaluate Drift and Set a Task Value**  
   Use a Notebook or SQL task to query the latest drift metric(s) for the relevant model/feature slice and compute a boolean (e.g., `drift_exceeded = TRUE/FALSE`).  
   Save that as a task value for downstream branching.

3. **Task B — If/Else Conditional to Gate Retraining**  
   Add an If/Else task that evaluates the task value and only runs the retraining branch when true.  
   This keeps your DAG clean and your retrain runs explainable in the Jobs UI.

4. **Retraining Branch — Train, Register, and (Optionally) Deploy**  
   - Run your training pipeline  
   - Log metrics  
   - Register the candidate to Unity Catalog Model Registry  
   - Optionally promote or shadow test depending on governance policy

### Why This Is Better Than a Simple Schedule

- ✅ Event-driven (no periodic polling), minimizing cost and noise  
- ✅ Explainable control flow (`If/Else`, `Run-if` dependencies) in Workflows without an external orchestrator  

> **Tip:** If your drift table updates frequently, add a minimum time between triggers and a short “wait after last change” window in the Table Update Trigger to avoid multiple retrains on closely spaced writes.

---

## Pattern 2 — SQL Alert → Serverless Function → Databricks Job (Lightweight External)

If you prefer a pure **alert-driven** posture:

1. **Create a DBSQL Alert**  
   Against the drift metrics table to emit only when drift breaches your threshold.

2. **Send the Alert to a Webhook**  
   For example: AWS Lambda, Azure Function, or GCP Cloud Function.  
   This avoids hosting your own long-lived API server.

3. **Webhook Calls the Databricks Jobs API**  
   Use:
   - `POST /api/2.0/jobs/run-now`  
   - or `POST /api/2.0/jobs/runs/submit`

This pattern preserves the **“alert fires → retrain”** workflow while acknowledging that alerts don’t directly trigger Jobs in-platform.

---

## Pattern 3 — Faster Loop with Incremental Monitor Refresh

If you need **higher timeliness**:

- Inference and Time Series monitors support incremental processing of appended data.
- You can refresh more frequently without full-scan costs.
- Combine with the **Table Update Trigger** above to keep retraining responsive without maintaining a streaming job.

---

## Practical Implementation Sketch (Pattern 1)

### Trigger
Table Update Trigger on `catalog.schema.<table>` with `ANY_UPDATED`;  
set `min_time_between_triggers_seconds` and `wait_after_last_change_seconds` as needed.

### Task A — Evaluate Drift
1. Query latest drift window for the model ID and dimension(s) you care about.  
2. Compare to your threshold(s).  
3. Set a task value:
   ```python
   dbutils.jobs.taskValues.set(key="drift_exceeded", value=True/False)
    ```

### Task B — If/Else Conditional

```python
{{tasks.evaluate_drift.values.drift_exceeded}} == true
```

### Task C — Retrain

Execute training pipeline, track with MLflow, register version, run validations.

## Notes Specific to Inference Classification

- Inference classification monitors compute performance and drift metrics (e.g., accuracy, F1, precision/recall).

- They can include fairness/bias metrics like statistical parity, equal opportunity, etc., when labels and metadata are provided.

- You can alert on any of these metrics (or custom SQL expressions) to gate retraining or raise incidents.

## When to Pick Which Pattern

| Scenario | Recommended Pattern |
| --- | --- |
| Purely Databricks, event-driven workflow with clear lineage | Table Update Trigger + If/Else |
| Alert-driven process using webhook | SQL Alert + Webhook |

## Minimal Code Snippet for Task A (Evaluate Drift)

Replace placeholders before running.

```python
from pyspark.sql import functions as F

DRIFT_TABLE = "catalog.schema.inference_drift_metrics"  # replace
MODEL_ID = "my_model_vX"  # replace
METRIC = "psi"  # replace with your metric name
THRESHOLD = 0.2  # replace with your threshold

df = (
    spark.table(DRIFT_TABLE)
    .filter(F.col("model_id") == MODEL_ID)
    .filter(F.col("metric_name") == METRIC)
    .orderBy(F.col("window_end_time").desc())
    .limit(1)
)

row = df.collect()[0]
drift_value = row["metric_value"]  # adapt to your schema
drift_exceeded = drift_value is not None and drift_value > THRESHOLD

dbutils.jobs.taskValues.set(key="drift_exceeded", value=bool(drift_exceeded))
print(f"Drift exceeded? {drift_exceeded} (value={drift_value}, threshold={THRESHOLD})")
```