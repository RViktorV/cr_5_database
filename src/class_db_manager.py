import psycopg2
from config import config
from tabulate import tabulate


class DBManager:
    """
    Класс для работы с базой данных PostgreSQL, содержащей информацию о работодателях и вакансиях.
    """

    def __init__(self):
        """
        Инициализация DBManager. Подключается к базе данных, используя параметры из файла конфигурации.
        """
        self.params = config()
        self.conn = psycopg2.connect(**self.params)
        self.cur = self.conn.cursor()

    def get_companies_and_vacancies_count(self):
        """
        Получает список всех компаний, их ID и количество вакансий у каждой компании.
        :return: Список кортежей (ID компании, название компании, количество вакансий).
        """
        self.cur.execute("""
            SELECT e.id, e.name, COUNT(v.id) as vacancies_count
            FROM employers e
            LEFT JOIN vacancies v ON e.id = v.employer_id
            GROUP BY e.id, e.name
        """)
        return self.cur.fetchall()

    def get_all_vacancies(self):
        """
        Получает список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию.
        :return: Список кортежей (название компании, название вакансии, зарплата от, зарплата до, URL).
        """
        self.cur.execute("""
            SELECT e.name, v.name, v.salary_from, v.salary_to, v.url
            FROM vacancies v
            JOIN employers e ON e.id = v.employer_id
        """)
        return self.cur.fetchall()

    def get_vacancies_by_company_id(self, company_id):
        """
        Получает список вакансий по ID компании.
        :param company_id: ID компании.
        :return: Список кортежей (название компании, название вакансии, зарплата от, зарплата до, ссылка на вакансию).
        """
        self.cur.execute("""
            SELECT e.name, v.name, v.salary_from, v.salary_to, v.url
            FROM vacancies v
            JOIN employers e ON v.employer_id = e.id
            WHERE v.employer_id = %s
        """, (str(company_id),))
        return self.cur.fetchall()

    def get_avg_salary(self):
        """
        Получает среднюю зарплату по вакансиям.
        :return: Средняя зарплата.
        """
        self.cur.execute("""
            SELECT ROUND(AVG((salary_from + salary_to) / 2))
            FROM vacancies
            WHERE salary_from IS NOT NULL AND salary_to IS NOT NULL
        """)
        return int(self.cur.fetchone()[0])

    def get_vacancies_with_higher_salary(self):
        """
        Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям.
        :return: Список кортежей (название компании, название вакансии, зарплата от, зарплата до, URL).
        """
        avg_salary = self.get_avg_salary()
        self.cur.execute("""
            SELECT e.name, v.name, v.salary_from, v.salary_to, v.url
            FROM vacancies v
            JOIN employers e ON e.id = v.employer_id
            WHERE (salary_from + salary_to) / 2 > %s
        """, (avg_salary,))
        return self.cur.fetchall()

    def get_vacancies_with_keyword(self, keyword):
        """
        Получает список всех вакансий, в названии которых содержатся переданные в метод слова.
        :param keyword: Ключевое слово для поиска в названии вакансий.
        :return: Список кортежей (название компании, название вакансии, зарплата от, зарплата до, URL).
        """
        self.cur.execute("""
            SELECT e.name, v.name, v.salary_from, v.salary_to, v.url
            FROM vacancies v
            JOIN employers e ON e.id = v.employer_id
            WHERE v.name ILIKE %s
        """, (f'%{keyword}%',))
        return self.cur.fetchall()

    def print_select(self, query):
        """
        Метод выводит результаты запроса в виде таблицы
        :param query:
        :return:
        """
        with self.conn.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
            colnames = [desc[0] for desc in cursor.description]

        table = tabulate(results, headers=colnames, tablefmt="grid")
        return '\n' + table

    def print_companies_and_vacancies_count(self):
        """
        Метод выводит компании и количество вакансий в виде таблицы.
        """
        results = self.get_companies_and_vacancies_count()
        colnames = ["Company ID", "Company", "Vacancies Count"]

        table = tabulate(results, headers=colnames, tablefmt="fancy_grid")
        return table

    def print_all_vacancies(self):
        """
        Метод выводит в виден таблицы список всех вакансий с указанием названия компании,
        названия вакансии и зарплаты и ссылки на вакансию.
        """
        results = self.get_all_vacancies()
        colnames = ['Company Name', 'Job Title', 'salary from', 'salary up to', 'URL']

        table = tabulate(results, headers=colnames, tablefmt="fancy_grid")
        return table

    def print_vacancies_with_higher_salary(self):
        """
        Метод выводит в виден таблицы вакансий, у которых зарплата выше средней по всем вакансиям.
        """
        results = self.get_vacancies_with_higher_salary()
        colnames = ['Company Name', 'Job Title', 'salary from', 'salary up to', 'URL']

        table = tabulate(results, headers=colnames, tablefmt="fancy_grid")
        return table

    def print_vacancies_with_keyword(self, keyword):
        """
        Метод выводит в виден таблицы вакансий, в названии которых содержатся переданные в метод ключевое слово
        """
        results = self.get_vacancies_with_keyword(keyword)
        colnames = ['Company Name', 'Job Title', 'salary from', 'salary up to', 'URL']

        table = tabulate(results, headers=colnames, tablefmt="fancy_grid")
        return table

    def print_vacancies_by_company_id(self, company_id):
        """
        Метод выводит в виде таблицы вакансии указанной компании по ее ID.
        :param company_id: ID компании.
        """
        results = self.get_vacancies_by_company_id(company_id)
        colnames = ['Company Name', 'Job Title', 'Salary From', 'Salary To', 'URL']

        table = tabulate(results, headers=colnames, tablefmt="fancy_grid")
        return table

    def close(self):
        """
        Закрывает соединение с базой данных.
        """
        self.cur.close()
        self.conn.close()
