import datetime

from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator, BigQueryDeleteTableOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow import DAG

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

default_dag_args = {
    'email': ['hardik.pruthi@mediaagility.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': YESTERDAY,
}

bq_dataset_name = 'analytics-323214.github_commits_dataset'
bq_github_table_id = bq_dataset_name + '.repo_watchlist_query'
output_file = 'gs://us-central1-composer-demo-32dddf6c-bucket/watchlist' + datetime.datetime.now().strftime(
    '%Y-%m-%d') + '.csv'

with DAG('repo_watch_count_dag', schedule_interval=datetime.timedelta(days=1), default_args=default_dag_args,) as dag:

    github_repo_watchlist_query = BigQueryExecuteQueryOperator(
        task_id='github_repo_watchlist_query',
        sql="""SELECT repo_name, watch_count FROM [analytics-323214:github_commits_dataset.repo_watchlist]
        where repo_name LIKE "%{{ dag_run.conf["repo_name"] if dag_run.conf.get("repo_name") else "" }}%" and watch_count > 
        {{ dag_run.conf["watch_count"] if dag_run.conf.get("watch_count") else 0 }}  """,
        destination_dataset_table=bq_github_table_id
    )

    export_watchlist_to_gcs = BigQueryToGCSOperator(
        task_id='export_watchlist_to_gcs',
        source_project_dataset_table=bq_github_table_id,
        destination_cloud_storage_uris=[output_file],
        export_format='CSV'
    )

    delete_temp_table = BigQueryDeleteTableOperator(
        task_id="delete_table",
        deletion_dataset_table=bq_github_table_id,
    )

    github_repo_watchlist_query >> export_watchlist_to_gcs >> delete_temp_table
