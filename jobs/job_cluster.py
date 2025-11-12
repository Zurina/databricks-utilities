import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs
from databricks.sdk.service.compute import ClusterSpec

w = WorkspaceClient()

notebook_path = f"/Users/{w.current_user.me().user_name}/random_numbers"

job_cluster_key = "my_job_cluster"

team = "alpha"

created_job = w.jobs.create(
    name=f"sdk-{time.time_ns()}",
    tags={"Team": team},
    job_clusters=[
        jobs.JobCluster(
            job_cluster_key=job_cluster_key,
            new_cluster=ClusterSpec(
                spark_version="14.3.x-scala2.12",
                node_type_id="Standard_DS3_v2",
                is_single_node=True
            )
        )
    ],
    tasks=[
        jobs.Task(
            description="my first sdk job",
            job_cluster_key=job_cluster_key,
            notebook_task=jobs.NotebookTask(notebook_path=notebook_path),
            task_key="test",
            timeout_seconds=0,
        )
    ],
)

run_by_id = w.jobs.run_now(job_id=created_job.job_id).result()

print(f"Created job with id={created_job.job_id}, run id={run_by_id.run_id}")
