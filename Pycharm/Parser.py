import pandas as pd
import numpy as np
from datetime import date
from datetime import datetime
import os
import glob
import shutil
from ozon_performance import OzonPerformance
from ozon_performance import DbWorking
from threading import Thread
import sys

# запись в БД
send_into_db = True
# удаление файлов по окончании
delete_files = True

# создаем рабочую папку, если еще не создана
if not os.path.isdir('data'):
    os.mkdir('data')
# создаем папку для сохранения отчетов
if not os.path.isdir('logs'):
    os.mkdir('logs')

# путь для сохранения файлов
path_ = r'./data/{}/'.format(str(date.today()))
if not os.path.isdir(path_):
    os.mkdir(path_)

# сохранение вывода в файл
stdoutOrigin = sys.stdout
sys.stdout = open("./logs/log_" + str(date.today()) + "_py.txt", "w")


# функция для записи пользовательского лога
def add_logging(data: str):
    log_file_name = 'log_' + str(date.today())
    log_path = './logs/'
    with open(f'{log_path}{log_file_name}.txt', 'a') as f:
        f.write(str(datetime.now()) + ': ')
        f.write(str(data + '\n'))


# параметры доступа к базе данных
host = 'rc1b-itt1uqz8cxhs0c3d.mdb.yandexcloud.net'
port = '6432'
ssl_mode = 'verify-full'
db_name = 'market_db'
user = 'sfedyusnin'
password = 'Qazwsx123Qaz'
target_session_attrs = 'read-write'

# таблица с данными
data_table_name = 'analitics_data2'

# sql запрос аккаунтов
api_perf_keys_resp = "select max(id),foo.client_id_performance, client_secret_performance\
                                    from (select distinct(client_id_performance) from account_list) as foo\
                                    join account_list\
                                    on foo.client_id_performance = account_list.client_id_performance\
                                    where (status_2 = 'Active' or status_1 = 'Active') and mp_id = 1\
                                    group by foo.client_id_performance, client_secret_performance\
                                    order by client_id_performance"

# создаем экземпляр класса, проверяем соединение с базой
db_access = f"host={host} " \
            f"port={port} " \
            f"sslmode={ssl_mode} " \
            f"dbname={db_name} " \
            f"user={user} " \
            f"password={password} " \
            f"target_session_attrs={target_session_attrs}"

working = DbWorking(db_access=db_access, keys_resp=api_perf_keys_resp, data_table_name=data_table_name)
connection = working.test_db_connection()

if connection is not None:
    add_logging(data=str(connection))

    # загружаем таблицы с данными и ключами
    working.get_analitics_data()
    api_keys = working.get_perf_keys()

    # загружаем данные из БД в переменную, загружаем из БД последнюю дату
    db_data = working.db_data
    last_date = str(working.get_last_date())
    print(last_date)

    add_logging(data='Количество записей в таблице аккаунтов ' + str(api_keys.shape[0]))
    add_logging(data='Количество записей в таблице статистики ' + str(db_data.shape[0]))
    add_logging(data='Дата последней записи в таблице статистики ' + str(last_date))

    # задаем диапазон дат
    date_from = last_date
    date_to = str(date.today())


    # функция для получения и сохранения отчетов
    def thread_func(*args):
        ozon = OzonPerformance(account_id=args[0], client_id=args[1], client_secret=args[2], day_lim=5, camp_lim=5)
        if ozon.auth:
            add_logging(data=f'Авторизация аккаунта id {args[0]} успешно')
            ozon.collect_data(date_from=date_from, date_to=date_to,
                              statistics=True, phrases=False, attribution=False, media=False, product=False,
                              daily=False, traffic=False)
            rep_ok = len([item for item in ozon.st_camp if item is not None])
            rep_lost = len([item for item in ozon.st_camp if item is None])
            add_logging(data=f'Аккаунт id {args[0]}, отчетов получено: {rep_ok}')
            add_logging(data=f'Аккаунт id {args[0]}, отчетов отказано: {rep_lost}')
            ozon.save_data(path_=path_,
                           statistics=True, phrases=False, attribution=False, media=False, product=False,
                           daily=False, traffic=False)
        else:
            add_logging(data=f'Авторизация аккаунта id {args[0]} не удалась')


    # создаем отдельные потоки по каждому аккаунту
    threads = []
    for index, keys in api_keys.iterrows():
        if len(keys[1]) > 0:
            client_id = keys[1]
            client_secret = keys[2]
            account_id = keys[0]
            threads.append(Thread(target=thread_func, args=(account_id, client_id, client_secret)))

    print(threads)

    # запускаем потоки
    for thread in threads:
        thread.start()

    # останавливаем потоки
    for thread in threads:
        thread.join()

else:
    add_logging(data='Нет подключения к БД')

# проверяем наличие загруженных файлов
files = []
for folder in os.listdir(path_):
    files += (glob.glob(os.path.join(path_ + folder + r'/statistics', "*.*")))

if len(files) != 0:
    # распаковываем архивы
    working.extract_zips(path_, rem=True)
    # создаем датасет на основе загруженных по API данных
    # dataset = working.make_dataset(path_=path_)
    dataset = working.make_dataset2(path_=path_)
    add_logging(data='Количество сырых строк ' + str(dataset.shape[0]))
    # обработаем пропуски
    dataset = dataset.fillna('nan')
    db_data = db_data.fillna('nan')
    dataset = dataset.replace('None', 'nan')
    db_data = db_data.replace('None', 'nan')
    # данные в базе после date_from отфильтрованные по загружаемым по api кампаниям
    db_data_from = db_data[db_data['data'] <= datetime.strptime(date_to, '%Y-%m-%d').date()]
    db_data_from = db_data_from[db_data_from['data'] > datetime.strptime(date_from, '%Y-%m-%d').date()]
    # db_data_from = db_data_from[db_data_from['data'] >= datetime.strptime(date_from, '%Y-%m-%d').date()]
    db_data_from = db_data_from[db_data_from['actionnum'].isin(dataset.actionnum.unique())]
    # колонки по которым происходит поиск совпадений
    cols = ['actionnum', 'data', 'request_type', 'viewtype', 'platfrom', 'views', 'clicks', 'ctr', 'audience', 'cpm',
            'expense', 'order_id', 'order_number', 'ozon_id', 'ozon_id_ad_sku', 'articul', 'name', 'orders', 'price',
            'revenue', 'search_price_perc', 'search_price_rur']
    # объединяем датасеты с удалением дубликатов
    into_db = pd.concat([db_data_from, dataset], axis=0).reset_index().drop('index',
                                                                            axis=1).drop_duplicates(subset=cols,
                                                                                                    keep=False)
    into_db = into_db.replace('nan', np.nan)
    # исключим записи из БД, оставим строки у которых нет id (так как это значение взято из БД)
    into_db = into_db[into_db['id'].isna()]

    print('dataset', dataset.shape)
    print('db_data_from', db_data_from.shape)
    print('into_db', into_db.shape)

    into_db.to_csv(path_ + 'into_db.csv', sep=';', index=False)
    add_logging(data='Готово строк для записи в БД: ' + str(into_db.shape[0]))
    print(into_db)

    if send_into_db is True:
        # отправляем в БД
        db_params = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"
        try:
            working.upl_to_db(dataset=into_db, db_params=db_params)
            add_logging(data='Запись в БД выполнена')
        except:
            add_logging(data='Запись в БД не удалась')
    else:
        add_logging(data='Запись в БД отключена')

else:
    add_logging(data='Нет загруженных файлов для обработки')

if delete_files is True:
    # удаляем файлы
    try:
        shutil.rmtree(path_)
        add_logging(data='Файлы удалены')
    except OSError as e:
        print("Error: %s - %s." % (e.filename, e.strerror))
        add_logging(data='Ошибка при удалении файлов')
else:
    add_logging(data='Удаление файлов отменено')

sys.stdout.close()
sys.stdout = stdoutOrigin
