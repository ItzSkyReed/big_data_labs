from os import getenv

from sqlalchemy import URL


class Settings:
    Host: str = 'postgres_logs_db_Individual_task'
    Port: int =  getenv('DB_PORT', 5432)
    Database: str = getenv('DB_NAME', 'user_logs_db')
    User: str = getenv('DB_USER', 'postgres')
    Password: str = getenv('DB_PASSWORD', '')

    @classmethod
    def database_url(cls) -> URL:
        return URL.create(
            drivername="postgresql+psycopg2",
            username=cls.User,
            password=cls.Password,
            host=cls.Host,
            port=cls.Port,
            database=cls.Database,
        )