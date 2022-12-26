import requests
import json
from datetime import datetime, date, timedelta
import time
import os
import pandas as pd
import numpy as np
import glob
import zipfile
import psycopg2
from sqlalchemy import create_engine


class OzonPerformanceEcom2:
    def __init__(self, client_id: str,
                 client_secret: str,
                 account_id=None
                 ):

        self.client_id = client_id
        self.client_secret = client_secret
        self.account_id = account_id

        self.auth = self.get_token()

        self.get_stat_report = []
        self.uuid_report = []

    def get_token(self):
        """
        Авторизация с получением токена
        """
        url = 'https://performance.ozon.ru/api/client/token'
        head = {"Content-Type": "application/json",
                "Accept": "application/json"
                }
        body = {"client_id": self.client_id,
                "client_secret": self.client_secret,
                "grant_type": "client_credentials"
                }
        response = requests.post(url, headers=head, data=json.dumps(body))
        if response.status_code == 200:
            print('Подключение успешно, токен получен')
            return response.json()
        else:
            print(response.text)
            return None

    def get_campaigns(self):
        """
        Получение кампаний
        """
        url = 'https://performance.ozon.ru:443/api/client/campaign'
        # url = 'https://performance.ozon.ru:443/api/client/campaign?state=CAMPAIGN_STATE_RUNNING'
        head = {"Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": self.auth['token_type'] + ' ' + self.auth['access_token']
                }
        response = requests.get(url, headers=head)
        if response.status_code == 200:
            print(f"Найдено {len(response.json()['list'])} кампаний")
            return response.json()['list']
        else:
            print(response.text)
            return None

    @staticmethod
    def get_camps_last_dates(user, password, host, port, db_name):
        """
        Получает таблицу с последними датами актуазизации статистики по кампаниям
        """
        query = """SELECT ad.actionnum as id, max(data) as last_stat_date 
                    FROM analitics_data2 ad 
                    GROUP BY actionnum 
                    ORDER BY actionnum
                    """
        db_params = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"
        engine = create_engine(db_params)
        try:
            return pd.read_sql(query, con=engine)
        except:
            print('Не удалось загрузить таблицу')
            return None

    def get_campaigns_info(self, user, password, host, port, db_name):
        """
        Создает DataFrame с параметрами кампаний и последней даты актуализации статистики
        """
        data = self.get_campaigns()
        if data is not None:
            camp_data = pd.DataFrame(data)
            camp_data['updatedAt'] = camp_data['updatedAt'].apply(
                lambda x: datetime.strptime(x.split('T')[0], '%Y-%m-%d').date())
            camp_data['createdAt'] = camp_data['createdAt'].apply(
                lambda x: datetime.strptime(x.split('T')[0], '%Y-%m-%d').date())
            camp_data['id'] = camp_data['id'].astype('int')

            stat = self.get_camps_last_dates(user, password, host, port, db_name)
            if stat is not None:
                stat['id'] = stat['id'].astype('int')
                return camp_data.merge(stat, how='left', left_on='id', right_on='id')
            else:
                return None
        else:
            return None

    @staticmethod
    def calc_date_from(row):
        state = row['state']
        last_stat_date = row['last_stat_date']
        updated_at = row['updatedAt']

        try:
            if (state == 'CAMPAIGN_STATE_RUNNING') or \
                    (state == 'CAMPAIGN_STATE_MODERATION_DRAFT') or \
                    (state == 'CAMPAIGN_STATE_MODERATION_IN_PROGRESS'):
                if last_stat_date is not np.nan and last_stat_date is not None:
                    if (date.today() - last_stat_date).days > 70:
                        date_from = date.today() - timedelta(days=70)
                    else:
                        date_from = last_stat_date + timedelta(days=1)
                else:
                    if (date.today() - updated_at).days < 70:
                        date_from = updated_at
                    else:
                        date_from = date.today() - timedelta(days=70)

            elif (state == 'CAMPAIGN_STATE_STOPPED') or \
                    (state == 'CAMPAIGN_STATE_INACTIVE'):
                if last_stat_date is not np.nan and last_stat_date is not None:
                    if (date.today() - last_stat_date).days > 30:
                        date_from = np.nan
                    else:
                        date_from = last_stat_date + timedelta(days=1)
                else:
                    if (date.today() - updated_at).days < 30:
                        date_from = updated_at
                    else:
                        date_from = date.today() - timedelta(days=70)

            elif (state == 'CAMPAIGN_STATE_PLANNED') or \
                    (state == 'CAMPAIGN_STATE_ARCHIVED') or \
                    (state == 'CAMPAIGN_STATE_MODERATION_FAILED') or \
                    (state == 'CAMPAIGN_STATE_FINISHED'):
                date_from = np.nan

            else:
                date_from = np.nan

        except:
            print('incorrect data')
            date_from = np.nan

        return date_from

    @staticmethod
    def split_list(list_: list, lenght: int):
        """Разбивает список на подсписки заданной длины (последний подсписок - остаток)"""
        if len(list_) >= lenght:
            data = []
            for i in range(0, len(list_), lenght):
                data.append(list(list_)[i:i + lenght])
        else:
            data = [list_]

        return data

    @staticmethod
    def split_time(date_from, date_to, n_days: int):
        """Разбивает интервал на подинтервалы заданной длины (последний подинтервал - остаток)"""
        delta = (date_to - date_from).days
        if delta > n_days:
            tms = []
            for t in range(0, delta, n_days):
                df = date_from + timedelta(days=t)
                to = date_from + timedelta(days=t + n_days - 1)
                if to >= date_to:
                    dt = date_to
                else:
                    dt = to
                tms.append([df, dt])
        else:
            tms = [[date_from, date_to]]

        return tms

    def get_camp_ranges(self, camp_info):
        """
        Вычисляет диапазон запрашиваемых дат статистики
        """
        camp_info['date_from'] = camp_info.apply(self.calc_date_from, axis=1)
        camp_info['date_to'] = camp_info[['date_from']].apply(
            lambda x: date.today() - timedelta(days=1) if x[0] is not np.nan else np.nan, axis=1)
        camp_info = camp_info.dropna(axis=0, subset=['date_from'])
        camp_info = camp_info[['id', 'date_from', 'date_to']]
        camp_info = camp_info.sort_values(by=['date_from'], ascending=[False])
        camp_groups_by_time = pd.DataFrame(camp_info.groupby(by=['date_from', 'date_to'])['id'].apply(list)).reset_index()
        camp_groups_by_time.rename(columns={'id': 'ids'}, inplace=True)
        return camp_groups_by_time

    def split_all_by_limits(self, camp_groups_by_time, campaign_lim=9, days_lim=30):
        """Разбивает таблицу запросов по кампаниям и временным интервалам"""

        c_res = []
        for keys, values in camp_groups_by_time.iterrows():
            data = self.split_list(list_=values['ids'], lenght=campaign_lim)
            for i in data:
                c_res.append({'date_from': values['date_from'], 'date_to': values['date_to'], 'ids': i})

        t_res = []
        for keys, values in pd.DataFrame(c_res).iterrows():
            time_ = self.split_time(date_from=values['date_from'], date_to=values['date_to'], n_days=days_lim)
            for tt in time_:
                t_res.append({'date_from': tt[0], 'date_to': tt[1], 'ids': values['ids']})

        return pd.DataFrame(t_res).sort_values(by=['date_to', 'date_from'], ascending=[False, False])

    def get_statistics(self,
                       campaigns: list,
                       date_from,
                       date_to,
                       group_by="DATE",
                       n_attempts=5,
                       delay=5
                       ):
        """Формирует запрос статистики, возвращает UUID и формат"""

        url = 'https://performance.ozon.ru:443/api/client/statistics'

        # auth = self.get_token()

        head = {
            # "Authorization": f"{auth['token_type']} {auth['access_token']}",
            "Authorization": f"{self.auth['token_type']} {self.auth['access_token']}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        body = {"campaigns": campaigns,
                "dateFrom": str(date_from),
                "dateTo": str(date_to),
                "groupBy": group_by
                }

        response = requests.post(url, headers=head, data=json.dumps(body))
        if response.status_code == 200:
            print('Campaign statistics UUID - ok')
            if len(campaigns) == 1:
                return {'UUID': response.json()['UUID'], 'format': 'csv'}
            else:
                return {'UUID': response.json()['UUID'], 'format': 'zip'}
        elif response.status_code == 429:
            n = 0
            while n < n_attempts:
                time.sleep(delay)
                response = requests.post(url, headers=head, data=json.dumps(body))
                print(f"status code {response.status_code}")
                if response.status_code == 200:
                    print('Campaign statistics UUID - ok')
                    if len(campaigns) == 1:
                        return {'UUID': response.json()['UUID'], 'format': 'csv'}
                    else:
                        return {'UUID': response.json()['UUID'], 'format': 'zip'}
                else:
                    n += 1
            print(f'Response declined {n_attempts} times')
            self.get_stat_report.append(
                {'account_id': self.account_id, 'date_from': date_from, 'date_to': date_to, 'ids': campaigns, 'status': 'declined'})
            return None
        else:
            self.get_stat_report.append(
                {'account_id': self.account_id, 'date_from': date_from, 'date_to': date_to, 'ids': campaigns, 'status': 'declined'})
            print(f'Error statistics {response.status_code}')
            return None

    @staticmethod
    def status_report(uuid, auth):
        """Возвращает статус отчета"""

        url = f"https://performance.ozon.ru:443/api/client/statistics/{uuid}"

        head = {"Authorization": f"{auth['token_type']} {auth['access_token']}",
                "Content-Type": "application/json",
                "Accept": "application/json"
                }

        response = requests.get(url, headers=head)

        if response.status_code == 200:
            return response.json()
        else:
            print(response.text)
            return None

    @staticmethod
    def download_report(auth, uuid):
        """Загружает файл отчета"""

        url = f"https://performance.ozon.ru:443/api/client/statistics/report?UUID={uuid}"

        head = {"Authorization": f"{auth['token_type']} {auth['access_token']}"}
        response = requests.get(url, headers=head)
        if response.status_code == 200:
            return response
        else:
            print(response.text)
            return None

    def get_report(self, uuid, format_, path):
        """Проверяет статус и по готовности загружает файл отчета"""

        auth = self.get_token()

        if auth is not None:
            try:
                status = None
                while status != 'OK':
                    time.sleep(10)
                    status = self.status_report(uuid, auth)['state']
                    print(f"{uuid} {status}")
                report = self.download_report(auth, uuid)

                if report is not None:
                    if format_ == 'csv':
                        file = f"{path}/{uuid}.csv"
                        with open(file, 'wb') as f:
                            f.write(report.content)
                        print(f"Saved {file}")
                    elif format_ == 'zip':
                        file = f"{path}/{uuid}.zip"
                        with open(file, 'wb') as f:
                            f.write(report.content)
                        with zipfile.ZipFile(file, 'r') as zf:
                            zf.extractall(path)
                        os.remove(file)
                    else:
                        return None
                    return 'OK'
                else:
                    print("Download error")
                    self.uuid_report.append({'account_id': self.account_id, 'uuid': uuid, 'format': format_, 'status': 'unsuccessfully'})
                    return None
            except Exception as e:
                print("Token expired or unknown error")
                print(e)
                self.uuid_report.append({'account_id': self.account_id, 'uuid': uuid, 'format': format_, 'status': 'unsuccessfully'})
                return None
        else:
            print("Auth error")
            self.uuid_report.append({'uuid': uuid, 'format': format_, 'status': 'unsuccessfully'})
            return None


class DbEcomru:
    def __init__(self,
                 host,
                 port,
                 ssl_mode,
                 db_name,
                 user,
                 password,
                 target_session_attrs
                 ):

        self.db_access = f"host={host} " \
                         f"port={port} " \
                         f"sslmode={ssl_mode} " \
                         f"dbname={db_name} " \
                         f"user={user} " \
                         f"password={password} " \
                         f"target_session_attrs={target_session_attrs}"

        self.db_params = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"

    def test_db_connection(self):
        """Проверка доступа к БД"""

        try:
            conn = psycopg2.connect(self.db_access)
            q = conn.cursor()
            q.execute('SELECT version()')
            connection = q.fetchone()
            print(connection)
            conn.close()
            return connection
        except:
            print('No connection to database')
            return None

    def get_accounts(self):
        """Загружает аккаунты ozon performance"""

        query = """
                SELECT al.id, asd.attribute_value key_attribute_value, asd2.attribute_value 
                FROM account_service_data asd 
                JOIN account_list al ON asd.account_id = al.id 
                JOIN (SELECT al.mp_id, asd.account_id, asd.attribute_id, asd.attribute_value 
                FROM account_service_data asd 
                JOIN account_list al ON asd.account_id = al.id WHERE al.mp_id = 14) asd2 
                ON asd2.mp_id = al.mp_id 
                AND asd2.account_id= asd.account_id AND asd2.attribute_id <> asd.attribute_id 
                WHERE al.mp_id = 14 
                AND asd.attribute_id = 9 
                AND al.status_1 = 'Active' 
                GROUP BY asd.attribute_id, asd.attribute_value, asd2.attribute_id, asd2.attribute_value, al.id 
                ORDER BY id
                """
        try:
            engine = create_engine(self.db_params)
            df = pd.read_sql(query, con=engine)
            print('Загружены performance_api_keys')
            return df

        except:
            print('No access to table, or lost connection')
            return None

    @staticmethod
    def stat_read_trans(file, api_id=np.nan, account_id=np.nan):
        """
        Обрабатывает датасет
        """
        data = pd.read_csv(file, sep=';', header=1,
                           skipfooter=1, engine='python'
                           )
        # data = data.dropna(axis=0, how='any', thresh=10)
        data = data.dropna(axis=0, thresh=10)
        camp = pd.read_csv(file, sep=';', header=0, nrows=0).columns[-1].split(',')[0].split()[-1]

        data['api_id'] = api_id
        data['account_id'] = account_id
        data['actionnum'] = camp

        columns = {'ID заказа': 'order_id',
                   'Номер заказа': 'order_number',
                   'Ozon ID': 'ozon_id',
                   'Ozon ID рекламируемого товара': 'ozon_id_ad_sku',
                   'Артикул': 'articul',
                   'Ставка, %': 'search_price_perc',
                   'Ставка, руб.': 'search_price_rur',
                   'Тип страницы': 'pagetype',
                   'Условие показа': 'viewtype',
                   'Показы': 'views',
                   'Клики': 'clicks',
                   'CTR (%)': 'ctr',
                   'Средняя ставка за 1000 показов (руб.)': 'cpm',
                   'Заказы модели': 'orders_model',
                   'Выручка с заказов модели (руб.)': 'revenue_model',
                   'Тип условия': 'request_type',
                   'Платформа': 'platfrom',
                   'Охват': 'audience',
                   'Баннер': 'banner',
                   'Средняя ставка (руб.)': 'avrg_bid',
                   'Расход за минусом бонусов (руб., с НДС)': 'exp_bonus',
                   'Дата': 'data',
                   'День': 'data',
                   'Наименование': 'name',
                   'Название товара': 'name',
                   'Количество': 'orders',
                   'Заказы': 'orders',
                   'Цена продажи': 'price',
                   'Цена товара (руб.)': 'price',
                   'Выручка (руб.)': 'revenue',
                   'Стоимость, руб.': 'revenue',
                   'Расход (руб., с НДС)': 'expense',
                   'Расход, руб.': 'expense',
                   'Unnamed: 1': 'empty',
                   'Средняя ставка за клик (руб.)': 'cpc',
                   'Ср. цена 1000 показов, ₽': 'cpm',
                   'Расход, ₽, с НДС': 'expense',
                   'Цена товара, ₽': 'price',
                   'Выручка, ₽': 'revenue',
                   'Выручка с заказов модели, ₽': 'revenue_model',
                   'Стоимость, ₽': 'revenue',
                   'Ставка, ₽': 'search_price_rur',
                   'Расход, ₽': 'expense',
                   'Средняя ставка, ₽': 'avrg_bid',
                   'Расход за минусом бонусов, ₽, с НДС': 'exp_bonus',
                   'Ср. цена клика, ₽': 'cpc',
                   'Средняя ставка (руб.)%!(EXTRA string=₽)': 'avrg_bid'
                   }

        data.rename(columns=columns, inplace=True)
        data['data'] = data['data'].apply(lambda x: datetime.strptime(x, '%d.%m.%Y').date())

        return data

    def make_dataset(self, path):
        """
        Собирает датасет
        """
        stat_data = []
        for folder in os.listdir(path):
            csv_files = glob.glob(os.path.join(f"{path}/{folder}/statistics", "*.csv"))
            for file in csv_files:
                try:
                    account_id = os.path.dirname(file).split('/')[-2].split('-')[0]
                    api_id = os.path.dirname(file).split('/')[-2].split('-')[1]
                    stat_data.append(self.stat_read_trans(file, api_id=api_id, account_id=account_id))
                except IndexError:
                    continue

        dataset = pd.concat(stat_data, axis=0).reset_index().drop('index', axis=1)

        dtypes = {
            'banner': 'str',
            'pagetype': 'str',
            'viewtype': 'str',
            'platfrom': 'str',
            'request_type': 'str',
            'sku': 'str',
            'name': 'str',
            'order_id': 'str',
            'order_number': 'str',
            'ozon_id': 'str',
            'ozon_id_ad_sku': 'str',
            'articul': 'str',
            'empty': 'str',
            'account_id': 'int',
            'views': 'float',
            'clicks': 'float',
            'audience': 'float',
            'exp_bonus': 'float',
            'actionnum': 'int',
            'avrg_bid': 'float',
            'search_price_rur': 'float',
            'search_price_perc': 'float',
            'price': 'float',
            'orders': 'float',
            'revenue_model': 'float',
            'orders_model': 'float',
            'revenue': 'float',
            'expense': 'float',
            'cpm': 'float',
            'ctr': 'float',
            'data': 'datetime',
            'api_id': 'str',
            'cpc': 'float'
        }

        for col in dataset.columns:
            if dtypes[col] == 'int' or dtypes[col] == 'float':
                dataset[col] = dataset[col].astype(str).str.replace(',', '.')
                dataset[col] = dataset[col].replace(r'^\s*$', np.nan, regex=True)
                dataset[col] = dataset[col].astype(dtypes[col], copy=False, errors='ignore')

            # if self.db_data[col].dtypes != dataset[col].dtypes:
            #     if (self.db_data[col].dtypes == 'float64' or self.db_data[col].dtypes == 'int64') and dataset[col].dtypes == 'object':
            #         dataset[col] = dataset[col].astype(str).str.replace(',', '.')
            #         dataset[col] = dataset[col].replace(r'^\s*$', np.nan, regex=True)
            #     dataset[col] = dataset[col].astype(self.db_data[col].dtypes)

        return dataset

    def upl_to_db(self, dataset, table_name):
        """Загружает данные в БД"""

        engine = create_engine(self.db_params)
        try:
            dataset.to_sql(name=table_name, con=engine, if_exists='append', index=False)
            print('Данные записаны в БД')
            return 'OK'
        except:
            print('Произошла непредвиденная ошибка')
            return None