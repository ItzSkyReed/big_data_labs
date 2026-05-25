from airflow.sdk import BaseHook
from sqlalchemy import URL


class Settings:
    AIRFLOW_CONN_ID = 'my_postgres_logs_conn'

    @classmethod
    def database_url(cls) -> URL:

        conn = BaseHook.get_connection(cls.AIRFLOW_CONN_ID)

        return URL.create(
            drivername="postgresql+psycopg2",
            username=conn.login,
            password=conn.password,
            host=conn.host,
            port=conn.port,
            database=conn.schema,
        )