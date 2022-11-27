from datetime import datetime
from datetime import date


def add_logging(logs_folder, data: str):
    """
    функция для записи пользовательского лога
    """
    log_file_name = 'log_' + str(date.today())
    with open(f'{logs_folder}/{log_file_name}.txt', 'a') as f:
        f.write(str(datetime.now()) + ': ')
        f.write(str(data + '\n'))
