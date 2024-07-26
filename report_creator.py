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
        - Дата: day
        - Количество входов на сайт: login_count
        - Количество выходов с сайта: logout_count
        - Количество действий внутри блога: blog_event_count

        :param: user_logs_df : pd.DataFrame
            DataFrame, содержащий логи пользователей с колонками:
            - `user_id`: Идентификатор пользователя.
            - `day`: Дата события.
            - `event_type_name`: Тип события (например: 'login', 'logout').
            - `space_type_name`: Тип пространства (например: 'blog').

        :return: pd.DataFrame
            DataFrame, содержащий следующие столбцы:
            - `user_id`: Идентификатор пользователя.
            - `day`: Дата события.
            - `blog_event_count`: Количество действий внутри блога (0, если данных нет).
            - `login_count`: Количество входов на сайт (0, если данных нет).
            - `logout_count`: Количество выходов с сайта (0, если данных нет).
        """
        user_logs_df['day'] = pd.to_datetime(user_logs_df['day'])

        # получение уникальных комбинаций `user_id` и `day`
        unique_user_day_df = user_logs_df[['user_id', 'day']].drop_duplicates()

        # получаем кол-во действий внутри блога
        blog_events = user_logs_df[user_logs_df['space_type_name'] == 'blog']
        blog_counts = blog_events.groupby(['user_id', 'day']).size().reset_index(name='blog_event_count')

        # получаем кол-во входов/выходов на сайт
        login_logout_events = user_logs_df[user_logs_df['event_type_name'].isin(['login', 'logout'])]
        login_logout_counts = login_logout_events.groupby(['user_id', 'day', 'event_type_name']).size().unstack(
            fill_value=0).reset_index()
        login_logout_counts.columns = ['user_id', 'day', 'login_count', 'logout_count']

        # формируем финальный датафрейм
        final_df = pd.merge(unique_user_day_df, blog_counts, on=['user_id', 'day'], how='left')
        final_df['blog_event_count'] = final_df['blog_event_count'].fillna(0).astype(int)

        final_df = pd.merge(final_df, login_logout_counts, on=['user_id', 'day'], how='left')
        final_df['login_count'] = final_df['login_count'].fillna(0).astype(int)
        final_df['logout_count'] = final_df['logout_count'].fillna(0).astype(int)

        # сортируем результат
        final_df = final_df.sort_values(by=['user_id', 'day'])

        return final_df

