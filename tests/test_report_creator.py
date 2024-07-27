import unittest
import pandas as pd
from pandas.testing import assert_frame_equal
from report_creator import ReportCreator


class TestReportCreator(unittest.TestCase):
    def setUp(self):
        self.data_one_user = {
            'user_id': [1] * 10,
            'datetime': ['2024-07-26']*3 + ['2024-07-27']*1 + ['2222-12-22']*4 + ['2000-02-02']*1 + ['2000-02-03']*1,
            'event_type_name': [
                'login', 'logout', 'login',
                'create_post',
                'create_post', 'logout', 'login', 'eat_corn',
                'logout',
                'coding',
            ],
            'space_type_name': [
                'global', 'global', 'global',
                'blog',
                'blog', 'global', 'global', 'real_life',
                'global',
                'programming',
            ]
        }
        self.one_user_logs_df = pd.DataFrame(self.data_one_user)

        self.data_many_user = {
            'user_id': [
                1, 1, 1, 1,
                2, 2, 2, 2,
                3, 3, 3, 3,
            ],
            'datetime': [
                '2024-07-26', '2024-07-26', '2024-08-10', '2024-08-10',
                '2024-07-26', '2024-07-26', '2024-07-26', '2024-07-28',
                '2024-07-26', '2024-07-26', '2024-07-27', '2024-07-28',
            ],
            'event_type_name': [
                'login', 'logout', 'login', 'create_post',
                'create_post', 'logout', 'run', 'win',
                'create_post', 'delete_post', 'delete_post', 'logout',
            ],
            'space_type_name': [
                'global', 'global', 'global', 'blog',
                'blog', 'global', 'life', 'good_life',
                'blog', 'blog', 'blog', 'global',
            ]
        }
        self.many_user_logs_df = pd.DataFrame(self.data_many_user)

    def test_empty_dataframe(self):
        """Проверяет, что пустой DataFrame возвращает правильную структуру"""
        df = pd.DataFrame(columns=['user_id', 'datetime', 'event_type_name', 'space_type_name'])
        expected_result = pd.DataFrame(columns=[
            'user_id', 'datetime', 'blog_event_count', 'login_count', 'logout_count'
        ])

        result = ReportCreator.create_general_user_logs_report(df)

        assert_frame_equal(result, expected_result)

    def test_one_user_report_creation(self):
        """Проверяет, что отчет для нескольких пользователей создается корректно"""
        expected_data = {
            'user_id': [1, 1, 1, 1],
            'datetime': ['2000-02-02', '2024-07-26', '2024-07-27', '2222-12-22'],
            'login_count': [0, 2, 0, 1],
            'logout_count': [1, 1, 0, 1],
            'blog_event_count': [0, 0, 1, 1]

        }

        expected_df = pd.DataFrame(expected_data, index=[3, 0, 1, 2])
        expected_df['datetime'] = pd.to_datetime(expected_df['datetime']).dt.date
        print("Ожидаем:")
        print(expected_df)

        result_df = ReportCreator.create_general_user_logs_report(self.one_user_logs_df)
        print("Получаем:")
        print(result_df)

        assert_frame_equal(result_df, expected_df)

    def test_many_users_report_creation(self):
        """Проверяет, что отчет для нескольких пользователей создается корректно"""
        expected_data = {
            'user_id': [1, 1, 2, 3, 3, 3],
            'datetime': ['2024-07-26', '2024-08-10', '2024-07-26', '2024-07-26', '2024-07-27', '2024-07-28'],
            'login_count': [1, 1, 0, 0, 0, 0],
            'logout_count': [1, 0, 1, 0, 0, 1],
            'blog_event_count': [0, 1, 1, 2, 1, 0]
        }

        expected_df = pd.DataFrame(expected_data, index=[0, 1, 2, 4, 5, 6])
        expected_df['datetime'] = pd.to_datetime(expected_df['datetime']).dt.date
        print("Ожидаем:")
        print(expected_df)

        result_df = ReportCreator.create_general_user_logs_report(self.many_user_logs_df)
        print("Получаем:")
        print(result_df)

        assert_frame_equal(result_df, expected_df)


if __name__ == '__main__':
    unittest.main()
