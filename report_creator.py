import pandas as pd
from pandas import DataFrame


class ReportCreator:
    @staticmethod
    def create_general_user_logs_report(user_logs_df: DataFrame) -> DataFrame:
        """
        Формирует отчет в формате DataFrame, сгруппированный по `user_id` и дате.
        В отчете содержится информация о количестве входов на сайт, выходов с сайта
        и действиях внутри блога для каждого пользователя за каждый день.

        Отчет включает следующие данные:
        - id пользователя user_id
        - Дата: datetime
        - Количество входов на сайт: login_count
        - Количество выходов с сайта: logout_count
        - Количество действий внутри блога: blog_event_count

        :param: user_logs_df : pd.DataFrame
            DataFrame, содержащий логи пользователей с колонками:
            - `user_id`: Идентификатор пользователя.
            - `datetime`: Дата события.
            - `event_type_name`: Тип события (например: 'login', 'logout').
            - `space_type_name`: Тип пространства (например: 'blog').

        :return: pd.DataFrame
            DataFrame, содержащий следующие столбцы:
            - `user_id`: Идентификатор пользователя.
            - `datetime`: Дата события.
            - `blog_event_count`: Количество действий внутри блога (0, если данных нет).
            - `login_count`: Количество входов на сайт (0, если данных нет).
            - `logout_count`: Количество выходов с сайта (0, если данных нет).
        """
        if user_logs_df.empty:
            # Возвращаем пустой DataFrame с нужными столбцами, если входной DataFrame пуст
            return pd.DataFrame(columns=[
                'user_id', 'datetime', 'blog_event_count', 'login_count', 'logout_count'
            ])

        user_logs_df['datetime'] = pd.to_datetime(user_logs_df['datetime']).dt.date

        # получение уникальных комбинаций `user_id` и `datetime`
        unique_user_datetime_df = user_logs_df[['user_id', 'datetime']].drop_duplicates()

        # получаем кол-во входов/выходов на сайт
        login_logout_events = user_logs_df[user_logs_df['event_type_name'].isin(['login', 'logout'])]
        # print(login_logout_events)

        login_logout_counts = login_logout_events.groupby(['user_id', 'datetime', 'event_type_name']).size().unstack(
            fill_value=0).reset_index()
        login_logout_counts = login_logout_counts.reindex(columns=['user_id', 'datetime', 'login', 'logout'], fill_value=0)

        login_logout_counts.columns = ['user_id', 'datetime', 'login_count', 'logout_count']

        # получаем кол-во действий внутри блога
        blog_events = user_logs_df[user_logs_df['space_type_name'] == 'blog']
        blog_counts = blog_events.groupby(['user_id', 'datetime']).size().reset_index(name='blog_event_count')

        # формируем финальный датафрейм
        final_df = pd.merge(unique_user_datetime_df, login_logout_counts, on=['user_id', 'datetime'], how='left')
        final_df['login_count'] = final_df['login_count'].fillna(0).astype(int)
        final_df['logout_count'] = final_df['logout_count'].fillna(0).astype(int)

        final_df = pd.merge(final_df, blog_counts, on=['user_id', 'datetime'], how='left')
        final_df['blog_event_count'] = final_df['blog_event_count'].fillna(0).astype(int)

        # Исключаем строки с нулевыми счетчиками
        final_df = final_df[
            (final_df['login_count'] > 0) |
            (final_df['logout_count'] > 0) |
            (final_df['blog_event_count'] > 0)
        ]

        # сортируем результат
        final_df = final_df.sort_values(by=['user_id', 'datetime'])

        return final_df

