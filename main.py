import psycopg2
from src.utils import (
    progress_bar, get_employers, get_vacancies, load_employers_to_db, load_vacancies_to_db,
    create_tables, add_description_column
)
from src.class_db_manager import DBManager
from config import config
from config import employer_ids


def main():
    print('''Данная программа будет получать данные о компаниях и вакансиях с сайта hh.ru,
загружать их в БД и  работать с этими данными
Ожидайте идет загрузка списка компаний с их id и количеством вакансий по ним:\n''')

    # Получаем данные о работодателях, включая их названия
    employers_data = get_employers(employer_ids)

    # Создание таблиц в БД
    create_tables()
    add_description_column()  # Добавить столбец description, если его нет

    # Получаем данные о вакансиях
    vacancies = {str(employer_id): get_vacancies(employer_id) for employer_id in employer_ids}

    # Инициализация переменной conn
    conn = None

    # Подключение к базе данных PostgresSQL
    params = config()
    try:
        conn = psycopg2.connect(**params)
        # Загрузка данных в базу данных
        load_employers_to_db(employers_data, conn)
        load_vacancies_to_db(vacancies, conn)

        # Работа с данными через класс DBManager
        db = DBManager()
        progress_bar(100)
        print(db.print_companies_and_vacancies_count())
        action = 1
        while action != 0:  # запускаем опросник
            action = input('\n1 - Вывести данные о работодателях на экран\n'
                           '2 - Вывести данные о вакансиях на экран\n'
                           '3 - Вывести список всех вакансий с указанием названия компании, названия вакансии и '
                           'зарплаты и ссылки на вакансию\n'
                           '4 - Вывести среднюю зарплату по вакансиям\n'
                           '5 - Вывести список всех вакансий, у которых зарплата выше средней по всем вакансиям\n'
                           '6 - Вывести список всех вакансий, в названии которых содержатся переданные слова\n'
                           '7 - Вывести список всех вакансий по id компании\n'
                           '0 - Выход\n')

            if action == "1":
                print(db.print_select("SELECT * FROM employers;"))
            elif action == "2":
                print(db.print_select("SELECT * FROM vacancies;"))
            elif action == "3":
                print(db.print_all_vacancies())
            elif action == "4":
                print('Средняя зарплата по вакансиям:\n', db.get_avg_salary())
            elif action == "5":
                print(db.print_vacancies_with_higher_salary())
            elif action == "6":
                keyword = input('Введите слово для фильтрации вакансий\n')
                print(db.print_vacancies_with_keyword(keyword))
            elif action == "7":
                company_id = int(input('Введите id компании, по которой вы хотите получить вакансии\n'))
                print(db.print_vacancies_by_company_id(company_id))
            elif action == "0":
                print("Досвидание")
                exit()

        db.close()
    except psycopg2.OperationalError as e:
        print(f"Unable to connect!\n{e}")
    finally:
        # Закрытие соединения с базой данных
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
