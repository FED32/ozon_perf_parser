#!/usr/bin/env python
# coding: utf-8



import requests
import json
from datetime import datetime
from datetime import timedelta
from datetime import date
import time
import os


# In[155]:


class Ozon_performance:
    def __init__(self, client_id, client_secret,
                 day_lim = 2,
                 camp_lim = 2):
        
        self.client_id = client_id
        self.client_secret = client_secret
        self.methods = {'statistics': 'https://performance.ozon.ru:443/api/client/statistics',
                        'phrases': 'https://performance.ozon.ru:443/api/client/statistics/phrases',
                        'attribution': 'https://performance.ozon.ru:443/api/client/statistics/attribution',
                        'media': 'https://performance.ozon.ru:443/api/client/statistics/campaign/media',
                        'product': 'https://performance.ozon.ru:443/api/client/statistics/campaign/product',
                        'daily': 'https://performance.ozon.ru:443/api/client/statistics/daily'}
        self.day_lim = day_lim
        self.camp_lim = camp_lim
        try:
            self.auth = self.get_token()
        except:
            print('Нет доступа к серверу')
        
#         self.date_to = str(date.today())
#         self.date_to = '2022-06-28'
#         self.date_from = '2022-07-01' 
        try:
            self.campaigns = [camp['id'] for camp in self.get_campaigns()]
            self.objects = {}
            for camp in self.campaigns:
                self.objects[camp]=[obj['id'] for obj in self.get_objects(campaign_id=camp)]
        except:
            print('Ошибка при получении кампаний')
        
        self.st_camp = []
        self.st_ph = []
        self.st_attr = []
        self.st_med = None
        self.st_pr = None
        self.st_dai = None

    def get_token(self):
        url = 'https://performance.ozon.ru/api/client/token'
        head = {"Content-Type" : "application/json",
                "Accept" : "application/json"
               }
        body = {"client_id" : self.client_id,
                "client_secret" : self.client_secret,
                "grant_type" : "client_credentials"
               }
        response = requests.post(url, headers=head, data=json.dumps(body))
        if response.status_code == 200:
            print('Подключение успешно, токен получен')
            return response.json()
        else:
            print(response.text)
        
    
    def get_campaigns(self):
        """
        Возвращает список кампаний
        """
        url = 'https://performance.ozon.ru:443/api/client/campaign'
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
    

    def get_objects(self, campaign_id):
        """
        Возвращает список рекламируемых объектов в кампании
        """
        url = f"https://performance.ozon.ru:443/api/client/campaign/{campaign_id}/objects"
        head = {"Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": self.auth['token_type'] + ' ' + self.auth['access_token']
               }
        response = requests.get(url, headers=head)
        if response.status_code == 200:
            return response.json()['list']
        else:
            print(response.text)
            
    def split_data(self, camp_lim):
        """
        Разбивает данные в соответствии с ограничениями Ozon
        """
        if len(self.objects) > camp_lim:
            data = []
            for i in range(0, len(self.objects), camp_lim):
                data.append(dict(list(self.objects.items())[i:i+camp_lim]))
        else:
            data = self.objects
        return data
    
    def split_time(self, date_from, date_to, day_lim):
        """
        Разбивает временной промежуток в соответствии с лимитом Ozon
        """
        delta = datetime.strptime(date_to, '%Y-%m-%d') - datetime.strptime(date_from, '%Y-%m-%d')
        if delta.days > day_lim:
            tms = []
            for t in range(0, delta.days, day_lim):
                dt_fr = str((datetime.strptime(date_from, '%Y-%m-%d') + timedelta(days = t)).date())
                if (datetime.strptime(date_from, '%Y-%m-%d') + timedelta(days = t + day_lim-1)).date() >= (datetime.strptime(date_to, '%Y-%m-%d')).date():
                    dt_to = str((datetime.strptime(date_to, '%Y-%m-%d')).date())
                else:
                    dt_to = str((datetime.strptime(date_from, '%Y-%m-%d') + timedelta(days = t + day_lim-1)).date())        
                tms.append([dt_fr, dt_to]) 
        else:
            tms = [[date_from, date_to]]
            
        return tms
    
    def get_statistics(self, campaigns,
                       t_date_from = None,
                       t_date_to= None,
                       group_by = "DATE",
                       n_attempts = 5,
                       delay = 3):
        """
        Возвращает статистику по кампании

        DATE — группировка по дате (по дням);
        START_OF_WEEK — группировка по неделям;
        START_OF_MONTH — группировка по месяцам.

        """
        url = self.methods['statistics']
        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token'],
               "Content-Type": "application/json",
               "Accept": "application/json"
               }
        body = {"campaigns": campaigns,
                "dateFrom": t_date_from,
                "dateTo": t_date_to,
                "groupBy": group_by
               }

        response = requests.post(url, headers=head, data=json.dumps(body))
        if response.status_code == 200:
            print('Статистика по кампаниям получена')
            if len(campaigns) == 1:
                return [response.json()['UUID'], 'csv']
            else:
                return [response.json()['UUID'], 'zip']
        elif response.status_code == 429:
            n = 0
            while n < n_attempts:
                time.sleep(delay)
                response = requests.post(url, headers=head, data=json.dumps(body))
                print('statistics, статус', response.status_code)
                if response.status_code == 200:
                    print('Статистика по кампаниям получена')
                    if len(campaigns) == 1:
                        return [response.json()['UUID'], 'csv']
                    else:
                        return [response.json()['UUID'], 'zip']
                    break
                else:
                    n+=1
        else:
            print(response.text)
            
    def get_phrases(self, objects,
                    t_date_from = None,
                    t_date_to= None,
                    group_by = "DATE",
                    n_attempts = 5,
                    delay = 3):
        """
        Возвращает отчет по фразам
        """
        url = self.methods['phrases']
        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token'],
                "Content-Type": "application/json",
                "Accept": "application/json"
               }   
        res = []
        for camp, obj in objects.items():
            if len(obj) != 0:
                body = {"campaigns": [camp],
                        "objects": obj,
                        "dateFrom": t_date_from,
                        "dateTo": t_date_to,
                        "groupBy": group_by
                       }
                response = requests.post(url, headers=head, data=json.dumps(body))
                if response.status_code == 200:
                    print('Статистика по фразам получена')
                    res.append([response.json()['UUID'], 'csv'])
                elif response.status_code == 429:
                    n = 0
                    while n < n_attempts:
                        time.sleep(delay)
                        response = requests.post(url, headers=head, data=json.dumps(body))
                        print('phrases, статус', response.status_code)
                        if response.status_code == 200:
                            print('Статистика по фразам получена')
                            res.append([response.json()['UUID'], 'csv'])
                            break
                        else:
                            n+=1  
                else:
                    print(response.text)
        return res

    def get_attribution(self, campaigns,
                        t_date_from = None,
                        t_date_to= None,
                        group_by = "DATE",
                        n_attempts = 5,
                        delay = 3):
        """
        Возвращает отчёт по заказам
        """
        url = self.methods['attribution']
        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token'],
                "Content-Type": "application/json",
                "Accept": "application/json"
               }
        body = {"campaigns": campaigns,
                "dateFrom": t_date_from,
                "dateTo": t_date_to,
                "groupBy": group_by
               }
        time.sleep(delay)
        response = requests.post(url, headers=head, data=json.dumps(body))
        if response.status_code == 200:
            print('Статистика по заказам получена')
            if len(campaigns) == 1:
                return [response.json()['UUID'], 'csv']
            else:
                return [response.json()['UUID'], 'zip']
        elif response.status_code == 429:
            n = 0
            while n < n_attempts:
                time.sleep(delay)
                response = requests.post(url, headers=head, data=json.dumps(body))
                print('attribution, статус', response.status_code)
                if response.status_code == 200:
                    print('Статистика по заказам получена')
                    if len(campaigns) == 1:
                        return [response.json()['UUID'], 'csv']
                    else:
                        return [response.json()['UUID'], 'zip']
                    break
                else:
                    n+=1
        else:
            print(response.text)
            
    def get_media(self, campaigns,
                  t_date_from = None,
                  t_date_to= None,
                  n_attempts = 10,
                  delay = 3):
        """
        Возвращает статистику по медийным кампаниям
        """
        url = self.methods['media']
        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token'],
                "Content-Type": "application/json",
                "Accept": "application/json"
               }
        params = {"campaigns": campaigns,
                  "dateFrom": t_date_from,
                  "dateTo": t_date_to
                 }
        response = requests.get(url, headers=head, params=params)
        if response.status_code == 200:
            print('Статистика по медиа получена')
            return response
        else:
            print(response.text)
            
    def get_product(self, campaigns,
                    t_date_from = None,
                    t_date_to= None,
                    n_attempts = 10,
                    delay = 3):
        """
        Возвращает статистику по продуктовым кампаниям
        """
        url = self.methods['product']
        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token'],
                "Content-Type": "application/json"
               }
        params = {"campaigns": campaigns,
                  "dateFrom": t_date_from,
                  "dateTo": t_date_to
                 }
        response = requests.get(url, headers=head, params=params)
        if response.status_code == 200:
            print('Статистика продуктовая получена')
            return response
        else:
            print(response.text)
       
    def get_daily(self, campaigns,
                  t_date_from = None,
                  t_date_to= None,
                  n_attempts = 10,
                  delay = 3):
        """
        Возвращает дневную статистику по кампаниям
        """
        url = self.methods['daily']
        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token'],
                "Content-Type": "application/json"
               }
        params = {"campaigns": campaigns,
                  "dateFrom": t_date_from,
                  "dateTo": t_date_to
                 }
        response = requests.get(url, headers=head, params=params)
        if response.status_code == 200:
            print('Статистика дневная получена')
            return response
        else:
            print(response.text)
    
    def status_report(self, uuid):
        """
        Возвращает статус отчета
        """
        url = 'https://performance.ozon.ru:443/api/client/statistics/' + uuid
        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token'],
                "Content-Type": "application/json",
                "Accept": "application/json"
               }
        response = requests.get(url, headers=head)
        if response.status_code == 200:
            return response
        else:
            print(response.text)
        
    def get_report(self, uuid):
        """
        Получить файл отчета
        """
        url = 'https://performance.ozon.ru:443/api/client/statistics/report?UUID=' + uuid
        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token']}
        response = requests.get(url, headers=head)
        if response.status_code == 200:
            return response
        else:
            print(response.text)
    
    def collect_data(self, date_from, date_to,
                     methods = {'statistics': True, 'phrases': True, 'attribution': True, 
                                      'media': True,'product': True, 'daily': True}):
        data = self.split_data(camp_lim = self.camp_lim)
        time = self.split_time(date_from = date_from, date_to = date_to, day_lim = self.day_lim)
        self.time = time
        self.date_from = date_from
        self.date_to = date_to
        if methods['statistics'] is True:
            self.st_camp = []
        if methods['phrases'] is True:
            self.st_ph = []
        if methods['attribution'] is True:
            self.st_attr = []
        if methods['media'] is True:
            self.st_med = self.get_media(self.campaigns, t_date_from=date_from, t_date_to=date_to)
        if methods['product'] is True:
            self.st_pr = self.get_product(self.campaigns, t_date_from=date_from, t_date_to=date_to)
        if methods['daily'] is True:
            self.st_dai = self.get_daily(self.campaigns, t_date_from=date_from, t_date_to=date_to)
        try:
            for d in data:
                for t in time:
                    if methods['statistics'] is True:
                        self.st_camp.append(self.get_statistics(list(d.keys()), t_date_from=t[0], t_date_to=t[1]))
                    if methods['phrases'] is True:
                        self.st_ph.append(self.get_phrases(d, t_date_from=t[0], t_date_to=t[1]))
                    if methods['attribution'] is True:
                        self.st_attr.append(self.get_attribution(list(d.keys()), t_date_from=t[0], t_date_to=t[1]))
        except TimeoutError:
            print('Нет ответа от сервера')
                
    def save_data(self, path = r'./data/', 
                  methods = {'statistics': True, 'phrases': True, 'attribution': True, 
                             'media': True,'product': True, 'daily': True}):
        if methods['media'] is True:
            if not os.path.isdir(path + 'media'):
                os.mkdir(path + 'media')
            name = path + r'media/' + f"{self.client_id}_" + f"media_{self.date_from}-{self.date_to}.csv"
            file = open(name, 'wb')
            file.write(self.st_med.content)
            file.close()
            print('Сохранен', name)
        if methods['product'] is True:
            if not os.path.isdir(path + 'product'):
                os.mkdir(path + 'product')
            name = path + r'product/' + f"{self.client_id}_" + f"product_{self.date_from}-{self.date_to}.csv"
            file = open(name, 'wb')
            file.write(self.st_pr.content)
            file.close()
            print('Сохранен', name)       
        if methods['daily'] is True:
            if not os.path.isdir(path + 'daily'):
                os.mkdir(path + 'daily')
            name = path + r'daily/' + f"{self.client_id}_" +  f"daily_{self.date_from}-{self.date_to}.csv"
            file = open(name, 'wb')
            file.write(self.st_dai.content)
            file.close()
            print('Сохранен', name)
        if methods['statistics'] is True:
            if not os.path.isdir(path + 'statistics'):
                os.mkdir(path + 'statistics')
            for num, camp in enumerate(self.st_camp):
                try:
                    status = ''
                    while status != 'OK':
                        time.sleep(1)
                        status = self.status_report(uuid=camp[0]).json()['state']
                        print(status) 
                    report = self.get_report(uuid=camp[0])
                    name = path + r'statistics/' + f"{self.client_id}_" + f"campaigns_{num}.{camp[1]}"
                    file = open(name, 'wb')
                    file.write(report.content)
                    file.close()
                    print('Сохранен', name)
                except:
                    continue
        if methods['phrases'] is True:
            if not os.path.isdir(path + 'phrases'):
                os.mkdir(path + 'phrases')
            for num, ph in enumerate(self.st_ph):
                try:
                    for n_camp, phrases in enumerate(ph):
                        try:
                            status = ''
                            while status != 'OK':
                                time.sleep(1)
                                status = self.status_report(uuid=phrases[0]).json()['state']
                                print(status)
                            report = self.get_report(uuid=phrases[0])
                            name = path + r'phrases/' + f"{self.client_id}_" + f"phrases_{num}_{n_camp}.{phrases[1]}"
                            file = open(name, 'wb')
                            file.write(report.content)
                            file.close()
                            print('Сохранен', name)
                        except:
                            continue
                except:
                    continue
        if methods['attribution'] is True:
            if not os.path.isdir(path + 'attribution'):
                os.mkdir(path + 'attribution')
            for num, attr in enumerate(self.st_attr):
                try:
                    status = ''
                    while status != 'OK':
                        time.sleep(1)
                        status = self.status_report(uuid=phrases[0]).json()['state']
                        print(status)
                    report = self.get_report(uuid=attr[0])
                    name = path + r'attribution/' + f"{self.client_id}_" + f"attr_{num}.{attr[1]}"
                    file = open(name, 'wb')
                    file.write(report.content)
                    file.close()
                    print('Сохранен', name)
                except:
                    continue                


