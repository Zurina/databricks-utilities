import os
import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

w = WorkspaceClient()

notebook_path = f"/Users/{w.current_user.me().user_name}/random_numbers"

team = "charlie"

cluster_id = os.environ["DATABRICKS_CLUSTER_ID"]
w.clusters.ensure_cluster_is_running(cluster_id)

print(f"Using existing cluster: {cluster_id}")

created_job = w.jobs.create(
    name=f"my-existing-cluster-job-{time.time_ns()}",
    tags={"Team": team},
    tasks=[
        jobs.Task(
            description="my first sdk job",
            existing_cluster_id=cluster_id,
            notebook_task=jobs.NotebookTask(notebook_path=notebook_path),
            task_key="test",
            timeout_seconds=0,
        )
    ],
)

run_by_id = w.jobs.run_now(job_id=created_job.job_id).result()

print(f"Created job with id={created_job.job_id}, run id={run_by_id.run_id}")
