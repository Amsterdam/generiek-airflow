from airflow import DAG
from common import default_args

from postgres_update_azure_token_operator import PostgresUpdateAzureTokenOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


def create_error(*args, **kwargs):
    """Generic error raiser."""
    raise Exception


with DAG(
    "testdag",
    default_args=default_args,
) as dag:
    sqls = [
        "SELECT * FROM public.covid_19_alcoholverkoopverbod;",
    ]
    pg_update_azure_token_test = PostgresUpdateAzureTokenOperator(
        task_id="pg_update_azure_token_test",
        postgres_conn_id="postgres_default",
        generated_postgres_conn_id="postgres_azure"
    )
    pgtest = PostgresOperator(
        task_id="pgtest", postgres_conn_id="postgres_azure", sql=sqls
    )
    pg_update_azure_token_test >> pgtest
