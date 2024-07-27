from typing import Optional

import pandas as pd
from pandas import DataFrame

from database_connection import DatabaseConnection


class LogsDatabaseRequests:
    @staticmethod
    def get_all_users_logs(db_con_to_logs_database: DatabaseConnection) -> DataFrame:
        """
        Получаем логи всех пользователей:
        - logs.user_id AS user_id (id пользователя)
        - DATE(logs.datetime) AS day (день совершения лога)
        - event_type.name AS event_type_name (тип действия)
        - space_type.name AS space_type_name (тип пространства)

        :param db_con_to_logs_database: DatabaseConnection к бд с логами `logs_database`
        :return: DataFrame - результат запроса
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
        - logs.user_id AS user_id (id пользователя)
        - DATE(logs.datetime) AS day (день совершения лога)
        - event_type.name AS event_type_name (тип действия)
        - space_type.name AS space_type_name (тип пространства)

        :param db_con_to_logs_database: DatabaseConnection к бд с логами `logs_database`
        :param user_id: id пользователя для которого нужно получить логи
        :return: DataFrame - результат запроса
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


class AuthorsDatabaseRequests:
    @staticmethod
    def get_author_id_by_login(db_con_to_authors_database: DatabaseConnection, login: str) -> Optional[int]:
        """
        Получаем author.id по author.login

        :param db_con_to_authors_database: DatabaseConnection к бд `authors_database`
        :return: Optional[int] - author.id, если такой логин есть
        """
        query = """
            SELECT
                DISTINCT id
            FROM author
            WHERE login=:login;
        """
        params = {'login': login}

        result = db_con_to_authors_database.get_all(query, params=params).fetchone()
        if result is None:
            return result

        return result[0]

    @staticmethod
    def get_all_authors_posts(db_con_to_authors_database: DatabaseConnection) -> DataFrame:
        """
        Получаем информацию о постах написанных всеми пользователями
        - author.id AS author_id (id автора поста)
        - author.login AS author_login (login автора поста)

        - blog.name AS blog_name (название блога)
        - blog.owner_id AS blog_owner_id (id владельца блога)
        - blog.owner_id->author.login AS blog_owner_login (название блога)

        - post.header AS post_header (заголовок поста)

        :param db_con_to_authors_database: DatabaseConnection к бд `authors_database`
        :return: DataFrame - результат запроса
        """
        query = """
            SELECT
                post_author.id AS author_id,
                post_author.login AS author_login,
                blog.name AS blog_name,
                blog_author.id AS blog_owner_id,
                blog_author.login AS  blog_owner_login,
                post.header AS post_header
            FROM post
                JOIN author post_author ON post.author_id = post_author.id
                JOIN blog ON post.blog_id = blog.id
                JOIN author blog_author ON blog.owner_id = blog_author.id
            ORDER BY author_login, blog_name DESC, header;
        """
        result = db_con_to_authors_database.get_all(query)
        return pd.DataFrame(result)

    @staticmethod
    def get_one_author_posts(db_con_to_authors_database: DatabaseConnection, login: str) -> DataFrame:
        """
        Получаем информацию о постах написанных пользователем по его логину
        - author.id AS author_id (id автора поста)
        - author.login AS author_login (login автора поста)

        - blog.name AS blog_name (название блога)
        - blog.owner_id AS blog_owner_id (id владельца блога)
        - blog.owner_id->author.login AS blog_owner_login (название блога)

        - post.header AS post_header (заголовок поста)

        :param db_con_to_authors_database: DatabaseConnection к бд `authors_database`
        :param login: логин автора
        :return: DataFrame - результат запроса
        """
        query = """
                SELECT
                    post_author.id AS author_id,
                    post_author.login AS author_login,
                    blog.name AS blog_name,
                    blog_author.id AS blog_owner_id,
                    blog_author.login AS  blog_owner_login,
                    post.header AS post_header
                FROM post
                    JOIN author post_author ON post.author_id = post_author.id AND post_author.login=:login
                    JOIN blog ON post.blog_id = blog.id
                    JOIN author blog_author ON blog.owner_id = blog_author.id
                ORDER BY author_login, blog_name DESC, header;
            """
        params = {'login': login}
        result = db_con_to_authors_database.get_all(query, params=params)
        return pd.DataFrame(result)

