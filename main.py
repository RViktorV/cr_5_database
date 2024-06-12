import json
import psycopg2
from src.utils import get_employers, get_vacancies, load_employers_to_db, load_vacancies_to_db, create_tables, add_description_column
from src.class_db_manager import DBManager
from config import config
from config import employer_ids


def main():

    # Выводим идентификаторы работодателей
    # employer_ids = [employer[0] for employer in employers]
    # print("Employer IDs:")
    # print(employer_ids)

    # Получаем данные о работодателях, включая их названия
    employers_data = get_employers(employer_ids)

    # Список кортежей с id и именами работодателей
    employer_names = [(employer['id'], employer['name']) for employer in employers_data]

    # Создание таблиц в БД
    create_tables()
    add_description_column()  # Добавить столбец description, если его нет

    # Выводим идентификаторы и названия работодателей
    # print("\nEmployer IDs and Names:")
    # for employer_id, employer_name in employer_names:
    #     print(f"ID: {employer_id}, Name: {employer_name}")

    # Получаем данные о вакансиях
    vacancies = {str(employer_id): get_vacancies(employer_id) for employer_id in employer_ids}

    # Инициализация переменной conn
    conn = None

    # Подключение к базе данных PostgreSQL
    params = config()
    try:
        conn = psycopg2.connect(**params)
        # Загрузка данных в базу данных
        load_employers_to_db(employers_data, conn)
        load_vacancies_to_db(vacancies, conn)

        # Работа с данными через класс DBManager
        db = DBManager()
        # print(db.get_companies_and_vacancies_count())
        # print(db.get_all_vacancies())
        # print(db.get_avg_salary())
        # print(db.get_vacancies_with_higher_salary())
        # db.get_vacancies_with_keyword('Python')
        # print(db.print_companies_and_vacancies_count())
        # print(db.print_select("SELECT * FROM employers;"))
        # print(db.print_select("SELECT * FROM vacancies;"))
        # print(db.print_all_vacancies())
        print(db.print_vacancies_with_keyword())
        db.close()
    except psycopg2.OperationalError as e:
        print(f"Unable to connect!\n{e}")
    finally:
        # Закрытие соединения с базой данных
        if conn:
            conn.close()



if __name__ == "__main__":
    main()
    # Получаем идентификаторы интересных работодателей
    # employers = search_employers('IT', 10)
