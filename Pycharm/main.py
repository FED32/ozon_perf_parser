import pandas as pd
import numpy as np
from datetime import date
from datetime import datetime
import os

from ozon_performance import Ozon_performance
from ozon_performance import db_working

# задаем рабочую папку
path_ = r'./data/test2/'
if not os.path.isdir(path_):
    os.mkdir(path_)

# создаем экземпляр класса, проверяем соединение с базой
working = db_working()
working.test_db_connection()

# загружаем таблицы с данными и ключами
working.get_analitics_data()
api_keys = working.get_perf_keys()

# загружаем данные из БД в переменную, загружаем из БД последнюю дату
db_data = working.db_data
last_date = str(working.get_last_date())
print(last_date)

date_from = last_date
date_to = str(date.today())

# #загружаем статистику по каждому аккаунту
# for index, keys in api_keys.iterrows():
#     if len(keys[1]) > 0:
#         client_id = keys[1]
#         client_secret = keys[2]
#         account_id = keys[0]
#         ozon = Ozon_performance(account_id, client_id, client_secret, day_lim = 5, camp_lim = 5)
#         if ozon.auth:
#             ozon.collect_data(date_from = date_from, date_to = date_to,
#                             methods = {'statistics': True, 'phrases': False, 'attribution': False,
#                                           'media': False,'product': False, 'daily': False})
#             ozon.save_data(path_ = path_, methods = {'statistics': True, 'phrases': False, 'attribution': False,
#                                                       'media': False,'product': False, 'daily': False})

# #распаковываем архивы
# working.extract_zips(path_, rem = True)

# создаем датасет на основе загруженных по API данных
dataset = working.make_dataset(path_ = path_)

# обработаем пропуски
dataset = dataset.fillna('nan')
db_data = db_data.fillna('nan')
dataset = dataset.replace('None', 'nan')
db_data = db_data.replace('None', 'nan')

# данные в базе после date_from отфильтрованные по загружаемым по api кампаниям
db_data_from = db_data[db_data['data'] <= datetime.strptime(date_to, '%Y-%m-%d').date()]
db_data_from = db_data_from[db_data_from['data'] >= datetime.strptime(date_from, '%Y-%m-%d').date()]
db_data_from = db_data_from[db_data_from['actionnum'].isin(dataset.actionnum.unique())]

# колонки по которым происходит поиск совпадений
cols = ['actionnum', 'data', 'request_type', 'viewtype', 'platfrom', 'views', 'clicks', 'ctr', 'audience', 'cpm', 'expense',
       'order_id', 'order_number', 'ozon_id', 'ozon_id_ad_sku', 'articul', 'name', 'orders', 'price', 'revenue', 'search_price_perc', 'search_price_rur'
        ]

# объединяем датасеты с удалением дубликатов
into_db = pd.concat([db_data_from, dataset], axis=0).reset_index().drop('index', axis=1).drop_duplicates(subset=cols, keep=False)
into_db = into_db.replace('nan', np.nan)

# исключим записи из БД, оставим строки у которых нет id (так как это значение взято из БД)
into_db = into_db[into_db['id'].isna()]


print('dataset', dataset.shape)
print('db_data_from', db_data_from.shape)
print('into_db', into_db.shape)

into_db.to_csv(path_+'into_db.csv', sep=';', index=False)

print(into_db)