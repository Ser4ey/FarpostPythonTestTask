import logging
import os

import config
from database_connection import DatabaseConnection
from database_requests import LogsDatabaseRequests, AuthorsDatabaseRequests
from report_creator import ReportCreator


def create_output_directory_if_not_exist():
    if not os.path.exists(config.output_directory_name):
        os.makedirs(config.output_directory_name)


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    host = config.POSTGRES_HOST
    port = config.POSTGRES_PORT
    authors_database = "authors_database"
    logs_database = "logs_database"
    user = config.POSTGRES_USER
    password = config.POSTGRES_PASSWORD

    logging.info(f"Скрипт для формирования отчётов. Отчёты хранятся в папке: {config.output_directory_name}")
    logging.info("Введите login пользователя:")
    user_login = input()

    authors_db_con = DatabaseConnection(host, port, authors_database, user, password)
    logs_db_con = DatabaseConnection(host, port, logs_database, user, password)
    user_id = AuthorsDatabaseRequests.get_author_id_by_login(authors_db_con, user_login)
    if user_id is None:
        logging.fatal(f"Пользователь {user_login} не найден!")
        logging.info(f"Проверьте правильность login")
        logging.fatal(f"Отчёты не будут сформированы")
        exit()
    else:
        logging.info(f"Пользователь {user_login} найден. id: {user_id}")

    # формирования отчёта comments.csv (заменён на posts.csv так как отчёт comments.csv сформировать не получится)
    logging.info(f"Формируем отчёт posts.csv")
    user_posts = AuthorsDatabaseRequests.get_one_author_posts(authors_db_con, user_login)
    user_posts.to_csv(os.path.join(config.output_directory_name, "posts.csv"), index=False)
    logging.info(f"Отчёт posts.csv успешно сформирован")

    # формирования отчёта general.csv
    logging.info(f"Формируем отчёт general.csv")
    user_logs = LogsDatabaseRequests.get_one_user_logs(logs_db_con, user_id)
    final_log_report = ReportCreator.create_general_user_logs_report(user_logs)
    final_log_report.to_csv(os.path.join(config.output_directory_name, "general.csv"), index=False)
    logging.info(f"Отчёт general.csv успешно сформирован")


if __name__ == "__main__":
    create_output_directory_if_not_exist()
    main()

