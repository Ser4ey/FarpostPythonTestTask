import logging
import os

import pandas as pd

import config
from database_connection import DatabaseConnection
from database_requests import DatabaseRequests
from report_creator import ReportCreator


def create_output_directory_if_not_exist():
    if not os.path.exists(config.output_directory_name):
        os.makedirs(config.output_directory_name)


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    pd.set_option('display.max_columns', None)

    host = config.POSTGRES_HOST
    port = config.POSTGRES_PORT
    database = "logs_database"
    user = config.POSTGRES_USER
    password = config.POSTGRES_PASSWORD

    user_id = int(input("Введите id пользователя:"))

    db_con = DatabaseConnection(host, port, database, user, password)

    user_logs = DatabaseRequests.get_one_user_logs(db_con, user_id)
    final_log_report = ReportCreator.create_general_user_logs_report(user_logs)

    final_log_report.to_csv(os.path.join(config.output_directory_name, "general.csv"), index=False)

    logging.info(f"Отчёт general.csv успешно сформирован")


if __name__ == "__main__":
    create_output_directory_if_not_exist()
    main()

