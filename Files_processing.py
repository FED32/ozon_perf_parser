import pandas as pd
import numpy as np
import os
import glob
import zipfile
import psycopg2

from datetime import date
from datetime import datetime
from contextlib import closing



class db_working:
    def __init__(self, db_access = """host=rc1b-itt1uqz8cxhs0c3d.mdb.yandexcloud.net\
                                    port=6432\
                                    sslmode=verify-full\
                                    dbname=market_db\
                                    user=sfedyusnin\
                                    password=Qazwsx123Qaz\
                                    target_session_attrs=read-write"""):        
        self.db_access = db_access
        
        # необходимые запросы к БД
        self.api_keys_resp = 'SELECT * FROM account_list'
        self.keys_dt_cols_resp = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'account_list'"
        self.an_dt_resp = 'SELECT * FROM analitics_data2'
        self.an_dt_cols_resp = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'analitics_data2'"
        self.api_perf_keys_resp = "select max(id),foo.client_id_performance, client_secret_performance\
                                    from (select distinct(client_id_performance) from account_list) as foo\
                                    join account_list\
                                    on foo.client_id_performance = account_list.client_id_performance\
                                    where mp_id = 1\
                                    group by foo.client_id_performance, client_secret_performance\
                                    order by client_id_performance"
        
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
        self.db_data = pd.read_sql(self.an_dt_resp, psycopg2.connect(self.db_access))
        print('Загружена analitics_data')
#         return self.db_data
    
    def get_last_date(self):
        """
        Возвращает последнюю дату записи в базе
        """
        db_data = self.db_data
        return db_data['data'].sort_values(ascending=False).values[0]
    
    def get_keys(self):
        """
        Загружает из базы таблицу ключей
        """
        try:
            df = pd.read_sql(self.api_keys_resp, psycopg2.connect(self.db_access)) 
            print('Загружены api_keys')
            return df
        except:
            print('Доступ к таблице запрещен')
            
    def get_perf_keys(self):
        '''
        Загружает ключи performance
        '''        
        try:
            df = pd.read_sql(self.api_perf_keys_resp, psycopg2.connect(self.db_access))
            print('Загружены performance_api_keys')
            return df
        except:
            print('Доступ к таблице запрещен')         
         
    def extract_zips(self, path = r'./data/'):
        """
        Распаковывает все zip в папках statistics папок аккаунтов
        """
        for folder in os.listdir(path):
            zip_files = glob.glob(os.path.join(path+folder+r'/statistics', "*.zip"))
            for file in zip_files:
                print(f'Распаковка {file}')
                with zipfile.ZipFile(file) as zf:
                    zf.extractall(path+folder+r'/statistics')
    
    def stat_read_trans(self, file, api_id=None, account_id=None):
        """
        Обрабатывает датасет
        """
        data = pd.read_csv(file, sep=';')
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
                      
    def make_dataset(self, path=r'./data/'):
        """
        Собирает датасет
        """
        stat_data = []
        for folder in os.listdir(path):
            csv_files = glob.glob(os.path.join(path+folder+r'/statistics', "*.csv"))
            for file in csv_files:
                try:
                    account_id = os.path.dirname(file).split('/')[-2].split('-')[0]
                    api_id = os.path.dirname(file).split('/')[-2].split('-')[1]
                    stat_data.append(self.stat_read_trans(file, api_id=api_id, account_id=account_id))
                except IndexError:
                    continue
        dataset = pd.concat(stat_data, axis=0).reset_index().drop('index', axis=1)
        dataset['data'] = dataset[['Дата', 'День']].fillna('nan').apply(lambda x: x[0] if x[1] == 'nan' else x[1], axis=1)
        dataset.drop(columns=['Дата', 'День'], inplace=True)
        dataset['name'] = dataset[['Наименование', 'Название товара']].fillna('nan').apply(lambda x: x[0] if x[1] == 'nan' else x[1], axis=1)
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
                                'Баннер': 'banner', 'Средняя ставка (руб.)': 'avrg_bid', 'Кампания':'actionnum',
                                'Расход за минусом бонусов (руб., с НДС)': 'exp_bonus'}, inplace=True)
                                
        dataset['data'] = dataset['data'].apply(lambda x: datetime.strptime(x, '%d.%m.%Y').date())
#        dataset['sku'] = dataset['sku'].fillna('nan')
        
        for col in dataset.columns:
            if self.db_data[col].dtypes == 'float64' or self.db_data[col].dtypes == 'int64':
                dataset[col] = dataset[col].str.replace(',', '.')
                dataset[col] = dataset[col].replace(r'^\s*$', np.nan, regex=True)
                dataset[col] = dataset[col].astype(self.db_data[col].dtypes)

        return dataset      