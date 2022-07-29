from airflow import DAG
from common import default_args

from postgres_update_azure_token_operator import PostgresUpdateAzureTokenOperator
from postgres_on_azure_operator import PostgresOnAzureOperator
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

    # This DAG demonstrates two routes:
    # - use PostgresOnAzureOperator and PostgresOnAzureHook.
    # - use PostgresUpdateAzureTokenOperator to generate a temporary connection
    #   containing the token that can then be used by PostgresOperator

    # use PostgresOnAzureOperator and PostgresOnAzureHook.
    # PostgresOnAzureOperator will call PostgresOnAzureHook under the hood,
    # and therefore it can use the postgres_default connection.
    pg_azure_test = PostgresOnAzureOperator(
        task_id="pg_azure_test", postgres_conn_id="postgres_default", sql=sqls
    )

    pg_azure_test

    # use PostgresUpdateAzureTokenOperator to generate a temporary connection
    # containing the token that can then be used by PostgresOperator
    # (this is a workaround so that modifying the PostgresHook is not needed)
    # The vanilla PostgresOperator cannot use our custom PostgresOnAzureHook,
    # and therefore it needs that patched connection.

    # pg_update_azure_token_test = PostgresUpdateAzureTokenOperator(
    #     task_id="pg_update_azure_token_test",
    #     postgres_conn_id="postgres_default",
    #     generated_postgres_conn_id="postgres_azure"
    # )
    # pgtest = PostgresOperator(
    #     task_id="pgtest", postgres_conn_id="postgres_azure", sql=sqls
    # )
    # pg_update_azure_token_test >> pgtest
