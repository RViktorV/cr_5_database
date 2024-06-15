from configparser import ConfigParser
import os

ROOT_DIR = os.path.dirname(__file__)

# Список ID компаний (employer_id)
employer_ids = [
    '1740',  # Яндекс
    '3529',  # Сбербанк
    '64174', # 2ГИС
    '2180',  # ОЗОН
    '15478',  # VK
    '3776',  # МТС
    '8620',  # rambler
    '3529', # СБЕР
    '78638',  # Тинькофф
    '1440683',  # RUTUBE
]

def config(filename="database.ini", section="postgresql"):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception(
            'Section {0} is not found in the {1} file.'.format(section, filename))
    return db
