import logging

from sqlalchemy import create_engine, text, CursorResult
from sqlalchemy.exc import SQLAlchemyError


class DatabaseConnection:
    def __init__(self, host: str, port: int, database: str, user: str, password: str):
        """
        Инициализирует соединение с базой данных.

        :param host: Адрес хоста базы данных.
        :param port: Порт базы данных.
        :param database: Имя базы данных.
        :param user: Имя пользователя базы данных.
        :param password: Пароль пользователя базы данных.
        """
        self.database = database
        try:
            connection_string = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
            self.engine = create_engine(connection_string)
            logging.info(f"Успешное подключение к базе данных: {database}")
        except Exception as er:
            logging.error(f"Ошибка при подключении к базе данных: {er}")
            raise

    def get_all(self, query: str, params: dict = None) -> CursorResult:
        """
        Выполняет SQL-запрос и возвращает результат.

        :param query: SQL-запрос для выполнения.
        :param params: Параметры SQL-запроса
        :return: CursorResult - результат запроса
        """
        try:
            with self.engine.connect() as connection:
                result = connection.execute(text(query), params)
                return result
        except SQLAlchemyError as er:
            logging.error(f"Ошибка выполнения SQL-запроса: {er}")
            raise

