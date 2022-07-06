#!/usr/bin/env python
# coding: utf-8



import pandas as pd
import numpy as np
import os
import glob
import zipfile
import psycopg2

from datetime import date
from contextlib import closing


class db_working:
    def __init__(self, db_access = """host=rc1b-itt1uqz8cxhs0c3d.mdb.yandexcloud.net                                    port=6432                                    sslmode=verify-full                                    dbname=market_db                                    user=sfedyusnin                                    password=Qazwsx123Qaz                                    target_session_attrs=read-write"""):        
        self.db_access = db_access
        
        # необходимые запросы к БД
        self.api_keys_resp = 'SELECT * FROM account_list'
        self.keys_dt_cols_resp = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'account_list'"
        self.an_dt_resp = 'SELECT * FROM analitics_data2'
        self.an_dt_cols_resp = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'analitics_data2'"
        
#         self.db_data = self.get_analitics_data()
        
    def test_db_connection(self):
        """
        Проверка доступа к БД
        """
        conn = psycopg2.connect(self.db_access)
        q = conn.cursor()
        q.execute('SELECT version()')
        print(q.fetchone())
        conn.close()
        
    def get_analitics_data(self):
        """
        Загружает таблицу из базы
        """
        print('Загрузка analitics_data')
        self.db_data = pd.read_sql(self.an_dt_resp, psycopg2.connect(self.db_access))
#         return self.db_data
    
    def get_last_date(self):
        """
        Возвращает последнюю дату записи в базе
        """
        db_data = self.db_data
        return db_data['data'].sort_values(ascending=False)[0]
    
    def get_keys(self):
        """
        Загружает из базы таблицу ключей
        """
        print('Загрузка api_keys')
        try:   
            return pd.read_sql(self.api_keys_resp, psycopg2.connect(self.db_access))
        except:
            print('Доступ к таблице запрещен')
             
    def extract_zips(self, path = r'./data/'):
        """
        Распаковывает все zip лежащие в пути path
        """
        zip_files = glob.glob(os.path.join(path, "*.zip"))
        for file in zip_files:
            print(f'Распаковка {file}')
            with zipfile.ZipFile(file) as zf:
                zf.extractall(path)
                
    def stat_read_trans(self, path, api_id=None, account_id=None):
        """
        Обрабатывает датасет
        """
        data = pd.read_csv(path, sep=';')
        data = data.reset_index()

        camp = data.keys()[-1].split(',')[0].split()[-1]
        data.columns = data[0:1].values.tolist()[0]
        data.drop(index=0, inplace=True)
        data.drop(data.tail(1).index, inplace=True)
        
        data['api_id'] = api_id
        data['account_id'] = account_id
        data['Кампания'] = camp

        data = data[data.columns[-1:].tolist()+data.columns[:-1].tolist()]
        data = data[data.columns.dropna()]
        data = data.dropna(axis=0, how='any', thresh=10)
        return(data)
                      
    def make_dataset(self, path=r'./data/statistics', api_id=None, account_id=None):
        """
        Собирает датасет
        """
        csv_files = glob.glob(os.path.join(path, "*.csv"))
        stat_data = []
        for file in csv_files:
            stat_data.append(self.stat_read_trans(file, api_id=api_id, account_id=account_id))
        dataset = pd.concat(stat_data, axis=0).reset_index().drop('index', axis=1)
        dataset['data'] = dataset[['Дата', 'День']].fillna('').apply(lambda x: x[0] if x[1] == '' else x[1], axis=1)
        dataset.drop(columns=['Дата', 'День'], inplace=True)
        dataset['name'] = dataset[['Наименование', 'Название товара']].fillna('').apply(lambda x: x[0] if x[1] == '' else x[1], axis=1)
        dataset.drop(columns=['Наименование', 'Название товара'], inplace=True)
        dataset['orders'] = dataset[['Количество', 'Заказы']].fillna('').apply(lambda x: x[0] if x[1] == '' else x[1], axis=1)
        dataset.drop(columns=['Количество', 'Заказы'], inplace=True)
        dataset['price'] = dataset[['Цена продажи', 'Цена товара (руб.)']].fillna('').apply(lambda x: x[0] if x[1] == '' else x[1], axis=1)
        dataset.drop(columns=['Цена продажи', 'Цена товара (руб.)'], inplace=True)
        dataset['revenue'] = dataset[['Выручка (руб.)', 'Стоимость, руб.']].fillna('').apply(lambda x: x[0] if x[1] == '' else x[1], axis=1)
        dataset.drop(columns=['Выручка (руб.)', 'Стоимость, руб.'], inplace=True)
        dataset['expense'] = dataset[['Расход (руб., с НДС)', 'Расход, руб.']].fillna('').apply(lambda x: x[0] if x[1] == '' else x[1], axis=1)
        dataset.drop(columns=['Расход (руб., с НДС)', 'Расход, руб.'], inplace=True)

        dataset.rename(columns={'ID заказа':'order_id', 'Номер заказа': 'order_number', 'Ozon ID': 'ozon_id',
                                'Ozon ID рекламируемого товара': 'ozon_id_ad_sku', 'Артикул': 'articul',
                                'Ставка, %': 'search_price_perc', 'Ставка, руб.': 'search_price_rur',
                                'Тип страницы': 'pagetype', 'Условие показа': 'viewtype', 'Показы': 'views',
                                'Клики': 'clicks', 'CTR (%)': 'ctr', 'Средняя ставка за 1000 показов (руб.)': 'cpm',
                                'Заказы модели': 'orders_model', 'Выручка с заказов модели (руб.)': 'revenue_model',
                                'Тип условия': 'request_type', 'Платформа': 'platfrom', 'Охват': 'audience',
                                'Баннер': 'banner', 'Средняя ставка (руб.)': 'avrg_bid', 'Кампания':'actionnum'}, inplace=True)

        return dataset      


