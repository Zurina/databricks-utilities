import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

w = WorkspaceClient()

notebook_path = f"/Users/{w.current_user.me().user_name}/random_numbers"

team = "beta"

created_job = w.jobs.create(
    name=f"beta-serverless-{time.time_ns()}",
    tags={"Team": team},
    usage_policy_id="36e91398-96d2-4429-9aa1-d036524ddb7a",  # Replace with your budget usage policy ID
    tasks=[
        jobs.Task(
            description="my first serverless job",
            notebook_task=jobs.NotebookTask(notebook_path=notebook_path),
            task_key="test",
            timeout_seconds=0,
        )
    ],
)

# run_by_id = w.jobs.run_now(job_id=created_job.job_id).result()

# print(f"Created job with id={created_job.job_id}, run id={run_by_id.run_id}")
