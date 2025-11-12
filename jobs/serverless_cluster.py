import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

w = WorkspaceClient()

notebook_path = f"/Users/{w.current_user.me().user_name}/random_numbers"

team = "alpha"

created_job = w.jobs.create(
    name=f"sdk-{time.time_ns()}",
    tags={"Team": team},
    # usage_policy_id="4c21e9c8-164f-49cd-9070-541740884f94",  # Replace with your budget usage policy ID
    tasks=[
        jobs.Task(
            description="my first serverless job",
            notebook_task=jobs.NotebookTask(notebook_path=notebook_path),
            task_key="test",
            timeout_seconds=0,
        )
    ],
)

run_by_id = w.jobs.run_now(job_id=created_job.job_id).result()

print(f"Created job with id={created_job.job_id}, run id={run_by_id.run_id}")
