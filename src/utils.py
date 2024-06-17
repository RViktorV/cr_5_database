import requests
from config import config
import psycopg2
import sys
import time


def get_employers(employer_ids):
    """
    Получает данные о работодателях с сайта hh.ru.

    :param employer_ids: Список идентификаторов работодателей.
    :return: Список данных о работодателях.
    """
    employers = []
    for employer_id in employer_ids:
        url = f'https://api.hh.ru/employers/{employer_id}'
        response = requests.get(url)
        if response.status_code == 200:
            employers.append(response.json())
    return employers


def get_vacancies(employer_id):
    """
    Получает список вакансий для заданного работодателя.

    :param employer_id: ID работодателя.
    :return: Список вакансий.
    """
    url = f'https://api.hh.ru/vacancies'
    params = {'employer_id': employer_id, 'per_page': 100}
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json()['items']
    else:
        raise Exception(f'Error {response.status_code}: {response.text}')


def create_tables():
    """Создает таблицы employers и vacancies в базе данных."""
    global conn
    params = config()
    try:
        conn = psycopg2.connect(**params)
        cur = conn.cursor()

        # Удаление таблиц, если они существуют
        cur.execute("DROP TABLE IF EXISTS vacancies;")
        cur.execute("DROP TABLE IF EXISTS employers;")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS employers (
                id VARCHAR(255) PRIMARY KEY,
                name VARCHAR(255),
                url VARCHAR(255),
                description TEXT
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS vacancies (
                id VARCHAR(255) PRIMARY KEY,
                employer_id VARCHAR(255),
                name VARCHAR(255),
                salary_from NUMERIC,
                salary_to NUMERIC,
                url VARCHAR(255),
                description TEXT,
                created_at TIMESTAMP,
                FOREIGN KEY (employer_id) REFERENCES employers (id)
            );
        """)
        conn.commit()
    except psycopg2.Error as e:
        print(f'Error: {e}')
    finally:
        if conn:
            conn.close()


def add_description_column():
    """Добавляет столбец description в таблицу vacancies, если его еще нет."""
    global conn
    params = config()
    try:
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute("""
            ALTER TABLE vacancies ADD COLUMN IF NOT EXISTS description TEXT;
        """)
        conn.commit()
    except psycopg2.Error as e:
        print(f'Error: {e}')
    finally:
        if conn:
            conn.close()


def load_employers_to_db(employers, conn):
    """
    Загружает данные о работодателях в базу данных PostgresSQL в таблицу employers.
    :param employers: Список данных о работодателях.
    :param conn: Соединение с базой данных PostgresSQL.
    """
    with conn.cursor() as cur:
        for employer in employers:
            cur.execute("""
                INSERT INTO employers (id, name, url, description)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, (employer['id'], employer['name'], employer['alternate_url'], employer.get('description')))
    conn.commit()


def load_vacancies_to_db(vacancies, conn):
    """
    Загружает данные о вакансиях в базу данных PostgresSQL в таблицу vacancies.
    :param vacancies: Словарь, где ключ - идентификатор работодателя, значение - список вакансий.
    :param conn: Соединение с базой данных PostgresSQL.
    """
    with conn.cursor() as cur:
        for employer_id, vacs in vacancies.items():
            for vacancy in vacs:
                cur.execute("""
                    INSERT INTO vacancies (id, employer_id, name, url, salary_from, salary_to, description, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    vacancy['id'],
                    employer_id,
                    vacancy['name'],
                    vacancy['alternate_url'],
                    vacancy['salary']['from'] if vacancy['salary'] else None,
                    vacancy['salary']['to'] if vacancy['salary'] else None,
                    vacancy.get('snippet', {}).get('responsibility', ''),
                    vacancy['created_at']
                ))
    conn.commit()


def progress_bar(total):
    """Функция вывода на экран прогресс бара загрузки вакансий"""
    bar_length = 50
    for i in range(total + 1):
        progress = i / total
        bar = '[' + '#' * int(progress * bar_length) + ' ' * (bar_length - int(progress * bar_length)) + ']'
        sys.stdout.write('\rЗагрузка: {}% {}'.format(int(progress * 100), bar))
        sys.stdout.flush()
        time.sleep(0.1)  # Эмулируем процесс загрузки
    print('\nЗагрузка завершена.')
