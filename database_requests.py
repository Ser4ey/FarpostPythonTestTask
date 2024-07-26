import pandas as pd
from pandas import DataFrame

from database_connection import DatabaseConnection


class DatabaseDataFrameRequests:
    @staticmethod
    def get_all_users_logs(db_con_to_logs_database: DatabaseConnection) -> DataFrame:
        """
        Получаем логи всех пользователей

        :param db_con_to_logs_database: DatabaseConnection к бд с логами `logs_database`
        :return: CursorResult - результат запроса
        """
        query = """
            SELECT
                logs.user_id AS user_id,
                DATE(logs.datetime) AS day,
                event_type.name AS event_type_name,
                space_type.name AS space_type_name
            FROM logs
                 JOIN event_type ON logs.event_type_id = event_type.id
                 JOIN space_type ON event_type.space_type_id = space_type.id;
        """
        result = db_con_to_logs_database.get_all(query)
        return pd.DataFrame(result)

    @staticmethod
    def get_one_user_logs(db_con_to_logs_database: DatabaseConnection, user_id: int) -> DataFrame:
        """
        Получаем логи конкретного пользователя по user_id

        :param db_con_to_logs_database: DatabaseConnection к бд с логами `logs_database`
        :param user_id: id пользователя для которого нужно получить логи
        :return: CursorResult - результат запроса
        """
        query = """
            SELECT
                logs.user_id AS user_id,
                DATE(logs.datetime) AS day,
                event_type.name AS event_type_name,
                space_type.name AS space_type_name
            FROM logs
                 JOIN event_type ON logs.event_type_id = event_type.id
                 JOIN space_type ON event_type.space_type_id = space_type.id
            WHERE logs.user_id = :user_id;
        """
        params = {'user_id': user_id}

        result = db_con_to_logs_database.get_all(query, params=params)
        return pd.DataFrame(result)

