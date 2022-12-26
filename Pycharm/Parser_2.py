import logger
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
import time
import os
import glob
import shutil
from threading import Thread
from ozon_performance_2 import OzonPerformanceEcom2, DbEcomru
import warnings
warnings.filterwarnings('ignore')

###############################
upl_into_db = 1
delete_files = 0
n_attemps = 5

data_folder = './data'
parser_reports_folder = '../reports'

###############################

logger = logger.init_logger()

print('upl_into_db: ', upl_into_db)
print('delete_files: ', delete_files)

# создаем папки, если еще не созданы
if not os.path.isdir(data_folder):
    os.mkdir(data_folder)

if not os.path.isdir(parser_reports_folder):
    os.mkdir(parser_reports_folder)

# путь для сохранения файлов
path_ = f"{data_folder}/{str(date.today())}"
if not os.path.isdir(path_):
    os.mkdir(path_)

# параметры доступа к базе данных
host = os.environ.get('ECOMRU_PG_HOST', None)
port = os.environ.get('ECOMRU_PG_PORT', None)
ssl_mode = os.environ.get('ECOMRU_PG_SSL_MODE', None)
db_name = os.environ.get('ECOMRU_PG_DB_NAME', None)
user = os.environ.get('ECOMRU_PG_USER', None)
password = os.environ.get('ECOMRU_PG_PASSWORD', None)
target_session_attrs = 'read-write'


def get_reports(account_id, client_id, client_secret):

    # создаем папки
    report_folder = f"{path_}/{account_id}-{client_id}/statistics"
    if not os.path.isdir(f'{path_}/{account_id}-{client_id}'):
        os.mkdir(f'{path_}/{account_id}-{client_id}')
    if not os.path.isdir(report_folder):
        os.mkdir(report_folder)

    # инициализируем экземпляр класса
    ozon = OzonPerformanceEcom2(client_id, client_secret, account_id)
    if ozon.auth:
        campaigns = ozon.get_campaigns_info(user, password, host, port, db_name)
        if campaigns is not None:
            camp_ranges = ozon.get_camp_ranges(campaigns)
            requests = ozon.split_all_by_limits(camp_ranges, campaign_lim=10, days_lim=70)

            responses = []
            for index_, keys_ in requests.iterrows():
                campaigns = keys_['ids']
                date_from = keys_['date_from']
                date_to = keys_['date_to']
                responses.append(
                    ozon.get_statistics(campaigns, date_from, date_to, group_by = "DATE", n_attempts = 5, delay = 5))

            rep_ok = len([item for item in responses if item is not None])
            rep_lost = len([item for item in responses if item is None])

            logger.info(f"{account_id}: get uuid- {rep_ok}, lost uuid - {rep_lost}")

            for resp in responses:
                if resp is not None:
                    report = ozon.get_report(uuid=resp['UUID'], format_=resp['format'], path=report_folder)
                    if report is not None:
                        logger.info(f"{resp['UUID']} - downloaded")
                    else:
                        logger.error(f"{resp['UUID']} - unsuccessfully")

            # сохраняем отчеты по работе с аккаунтом
            account_folder = f"{parser_reports_folder}/{account_id}-{client_id}"
            if not os.path.isdir(account_folder):
                os.mkdir(account_folder)

            pd.DataFrame(ozon.get_stat_report).to_csv(f"{account_folder}/get_stat_report.csv", sep=';', index=False)
            pd.DataFrame(ozon.uuid_report).to_csv(f"{account_folder}/uuid_report.csv", sep=';', index=False)
        else:
            logger.error(f"{account_id} - no campaigns found, or error during loading campaigns")
    else:
        logger.error(f"{account_id} - auth failed")


database = DbEcomru(host=host,
                    port=port,
                    ssl_mode=ssl_mode,
                    db_name=db_name,
                    user=user,
                    password=password,
                    target_session_attrs=target_session_attrs)

connection = database.test_db_connection()

if connection is not None:
    logger.info("connection to db - ok")
    api_keys = database.get_accounts().drop_duplicates(subset=['key_attribute_value', 'attribute_value'], keep='first')
    if api_keys is not None:
        logger.info(f"accounts found: {str(api_keys.shape[0])}")

        # создаем отдельные потоки по каждому аккаунту
        threads = []
        for index, keys in api_keys.iterrows():

            if len(keys[1]) > 0:
                client_id = keys[1]
                client_secret = keys[2]
                account_id = keys[0]
                threads.append(Thread(target=get_reports, args=(account_id, client_id, client_secret)))

        print(threads)

        # запускаем потоки
        for thread in threads:
            thread.start()

        # останавливаем потоки
        for thread in threads:
            thread.join()

    else:
        logger.error('No access to accounts table, or lost connection')
else:
    logger.error("no database connection")

# проверяем наличие загруженных файлов
files = []
for folder in os.listdir(path_):
    files += (glob.glob(os.path.join(f"{path_}/{folder}/statistics", "*.csv")))
# print(path_)
# print(files)
if len(files) > 0:
    # создаем датасет на основе загруженных по API данных
    dataset = database.make_dataset(path=path_)
    print('dataset', dataset.shape)

    cols = dataset.columns.tolist()
    dataset = dataset.drop_duplicates(subset=cols, keep='first')
    print('dataset', dataset.shape)

    print(dataset)
    dataset.to_csv(f'{path_}/into_db.csv', sep=';', index=False)

    if upl_into_db == 1:
        n = 0
        while n < n_attemps:
            upload = database.upl_to_db(dataset=dataset, table_name='analitics_data2')
            if upload is not None:
                logger.info("Upload to db successful")
                break
            else:
                time.sleep(5)
                n += 1
    else:
        logger.info('Upl to db canceled')
else:
    logger.info("No files")

if delete_files == 1:
    try:
        shutil.rmtree(path_)
        logger.info('Files (folder) deleted')
    except OSError as e:
        print("Error: %s - %s." % (e.filename, e.strerror))
        logger.error('Error deleting')
else:
    logger.info('Delete canceled')






