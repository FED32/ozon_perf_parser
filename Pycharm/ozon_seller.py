import requests
import json


class OzonSeller:
    def __init__(self, client_id, api_key,
                 url_='https://api-seller.ozon.ru',
                 action_list_='/v1/actions',
                 candidates_='/v1/actions/candidates',
                 deactivate_='/v1/actions/products/deactivate'):
        self.client_id = client_id
        self.api_key = api_key
        self.url = url_
        self.action_list = action_list_
        self.candidates = candidates_
        self.deactivate = deactivate_
        self.head = {
            "Client-Id": client_id,
            "Api-Key": api_key,
            "Content-Type": "application/json"
        }

    def get_actions_list(self):
        """
        Возвращает список акций
        """
        response = requests.get(self.url + self.action_list, headers=self.head)
        if response.status_code == 200:
            return response.json()["result"]
        else:
            return response.text

    def get_candidates(self, action_id, limit=100):
        """
        Возвращает все товары доступные для акций
        """
        offset = 0
        res = []
        while True:
            body = {"action_id": action_id, "limit": limit, "offset": offset}
            response = requests.post(self.url + self.candidates, headers=self.head, data=json.dumps(body))
            if response.status_code == 200:
                if len(response.json()["result"]["products"]) > 0:
                    res += (response.json()["result"]["products"])
                    offset += limit
                else:
                    return res
                    # break
            else:
                return response.text
                # break

    def del_product(self, action_id, product_ids):
        """
        Удаление товаров из акции
        """
        body = {
            "action_id": action_id,
            "product_ids": product_ids
        }
        response = requests.post(self.url + self.deactivate, headers=self.head, data=json.dumps(body))
        if response.status_code == 200:
            return response.json()["result"]
        else:
            return response.text

    def products_list(self):
        """
        Список товаров
        """
        url = 'https://api-seller.ozon.ru/v2/product/list'
        body = {"filter": {},
                "last_id": "",
                "limit": 1000
                }
        response = requests.post(url, headers=self.head, data=json.dumps(body))
        if response.status_code == 200:
            return response.json()["result"]
        else:
            return response.text

#     def products_info(self, ):
#         url = 'https://api-seller.ozon.ru/v2/product/info'
