import pandas as pd
import numpy as np
from datetime import date
from datetime import datetime
from datetime import timedelta
import os
import glob
import shutil
from ozon_performance import OzonPerformance
from ozon_performance import DbWorking
import sys
from pathlib import Path
from data_logging import add_logging


def add_new_user_data(client_id,
                      client_secret,
                      account_id,
                      send_into_db=0,
                      delete_files=0,
                      days=30,
                      data_folder_name='/data_new_user',
                      logs_folder_name='/logs'
                      ):
    """
    Проверяет наличие записей в базе и добавляет последнюю статистику при отсутствии
    """

    # параметры доступа к базе данных
    host = os.environ.get('ECOMRU_PG_HOST', None)
    port = os.environ.get('ECOMRU_PG_PORT', None)
    ssl_mode = os.environ.get('ECOMRU_PG_SSL_MODE', None)
    db_name = os.environ.get('ECOMRU_PG_DB_NAME', None)
    user = os.environ.get('ECOMRU_PG_USER', None)
    password = os.environ.get('ECOMRU_PG_PASSWORD', None)
    target_session_attrs = 'read-write'

    base_path = str(Path('Parser.py').resolve().parent.parent)
    print(base_path)

    data_folder = base_path + data_folder_name
    logs_folder = base_path + logs_folder_name

    if not os.path.isdir(data_folder):
        os.mkdir(data_folder)
    if not os.path.isdir(logs_folder):
        os.mkdir(logs_folder)

    # путь для сохранения файлов
    path_ = data_folder + f'/{str(date.today())}/'
    if not os.path.isdir(path_):
        os.mkdir(path_)

    # создаем экземпляр класса, проверяем соединение с базой
    db_access = f"host={host} " \
                f"port={port} " \
                f"sslmode={ssl_mode} " \
                f"dbname={db_name} " \
                f"user={user} " \
                f"password={password} " \
                f"target_session_attrs={target_session_attrs}"

    db_params = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"

    working = DbWorking(db_access=db_access, data_table_name='analitics_data2')
    connection = working.test_db_connection()

    if connection is not None:
        add_logging(logs_folder, data=str(connection))

        # # загружаем таблицу с данными
        working.get_analitics_data_head(db_params=db_params)
        # db_data = working.db_data

        api_id = client_id.split('-')[0]
        query = f"SELECT * FROM analitics_data2 WHERE api_id = '{api_id}'"

        db_data = working.get_data_by_response(sql_resp=query, db_params=db_params)

        if db_data is not None:
            if db_data.shape[0] == 0:
                date_from = str(date.today() - timedelta(days=days))
                date_to = str(date.today())
                ozon = OzonPerformance(account_id=account_id, client_id=client_id, client_secret=client_secret,
                                       day_lim=5, camp_lim=5)
                if ozon.auth is not None:
                    add_logging(logs_folder, data=f'Авторизация аккаунта id {account_id} успешно')
                    ozon.collect_data(date_from=date_from, date_to=date_to,
                                      statistics=True, phrases=False, attribution=False, media=False, product=False,
                                      daily=False, traffic=False)
                    rep_ok = len([item for item in ozon.st_camp if item is not None])
                    rep_lost = len([item for item in ozon.st_camp if item is None])
                    add_logging(logs_folder, data=f'Аккаунт id {account_id}, отчетов получено: {rep_ok}')
                    add_logging(logs_folder, data=f'Аккаунт id {account_id}, отчетов отказано: {rep_lost}')
                    ozon.save_data(path_=path_,
                                   statistics=True, phrases=False, attribution=False, media=False, product=False,
                                   daily=False, traffic=False)
                else:
                    add_logging(logs_folder, data=f'Авторизация аккаунта id {account_id} не удалась')
            else:
                print('Данные уже имеются в базе')
                add_logging(logs_folder, data='Данные уже имеются в базе')
        else:
            print('Ошибка при загрузке')
            add_logging(logs_folder, data='Ошибка при загрузке')
    else:
        add_logging(logs_folder, data='Нет подключения к БД')

    # проверяем наличие загруженных файлов
    files = []
    for folder in os.listdir(path_):
        files += (glob.glob(os.path.join(path_ + folder + r'/statistics', "*.*")))

    if len(files) != 0:
        # распаковываем архивы
        working.extract_zips(path_, rem=True)

        # создаем датасет на основе загруженных по API данных
        dataset = working.make_dataset2(path_=path_)
        add_logging(logs_folder, data=f'Количество строк загружено {dataset.shape[0]}')
        # заполним пропуски
        dataset = dataset.fillna(np.nan)
        print(dataset)
        dataset.to_csv(path_ + 'dataset.csv', sep=';', index=False)

        if send_into_db == 1:
            try:
                working.upl_to_db(dataset=into_db, db_params=db_params)
                add_logging(logs_folder, data='Запись в БД выполнена')
            except:
                add_logging(logs_folder, data='Запись в БД не удалась')
        else:
            print('Запись в БД отключена')
            add_logging(logs_folder, data='Запись в БД отключена')

    if delete_files == 1:
        # удаляем файлы
        try:
            shutil.rmtree(path_)
            add_logging(logs_folder, data='Файлы удалены')
        except OSError as e:
            print("Error: %s - %s." % (e.filename, e.strerror))
            add_logging(logs_folder, data='Ошибка при удалении файлов')
    else:
        add_logging(logs_folder, data='Удаление файлов отменено')

    return 'OK'










