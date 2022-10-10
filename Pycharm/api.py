from flask import Flask, Request, jsonify
from flask_restful import Api, Resource, reqparse
from ozon_performance import OzonPerformance
import json
from configparser import ConfigParser
from flasgger import Swagger, swag_from
import config

# читаем конфиг
urls = ConfigParser()
urls.read("api_config.ini")
host = urls["params_1"]["host"]
port = urls["params_1"]["port"]
print('host: ', host)
print('port: ', port)


app = Flask(__name__)

# Create an APISpec
template = {"swagger": "2.0",
            "info": {"title": "ECOMSELLER OZON Performance API",
                     "description": "API для взаимодействия с кабинетом OZON Performance",
                     "version": "0.1.0",
                     "contact": {"name": "sfedyushin",
                                 "url": "https://ecomru.ru/"}
                     }
            }

app.config['SWAGGER'] = {'title': 'API for OZON Performance',
                         'uiversion': 3,
                         "specs_route": "/apidocs/"}
swagger = Swagger(app, template=template)
app.config.from_object(config.Config)

api = Api(app)


class Main(Resource):

    # @swag_from('defs.yml', validation=True)
    def post(self, method):
        """
        ---
        tags:
        - Flast Restful APIs
        parameters:
            - method: campaigns
              in: data
              type: string
              required: true
            - name: client_id
              in: data
              type: string
              required: true
              description: client_id
            - name: client_secret
              in: query
              type: string
              required: true
              description: client_secret
            - name: campaign_id
              in: query
              type: string
              # required: true
              description: campaign_id
        #
        #
        # responses:
        #     500:
        #       description: Error The number is not integer!
        #     200:
        #       description: Number statistics
        #       schema:
        #         id: stats
        #         properties:
        #           sum:
        #             type: integer
        #             description: The sum of number
        #           product:
        #             type: integer
        #             description: The sum of number
        #           division:
        #             type: integer
        #             description: The sum of number

        """
        if method == "campaigns":
            # список кампаний
            parser = reqparse.RequestParser()
            parser.add_argument("client_id", required=True)
            parser.add_argument("client_secret", required=True)
            params = parser.parse_args()
            # print(params)
            ozon = OzonPerformance(client_id=params["client_id"],
                                   client_secret=params["client_secret"])
            if ozon.auth is None:
                return {'error': 'Ошибка авторизации'}
            else:
                return {'result': ozon.get_campaigns()}

        if method == 'objects':
            # список рекламируемых объектов в кампании
            parser = reqparse.RequestParser()
            parser.add_argument("client_id", required=True)
            parser.add_argument("client_secret", required=True)
            parser.add_argument("campaign_id", required=True)
            params = parser.parse_args()
            # print(params)
            ozon = OzonPerformance(client_id=params["client_id"],
                                   client_secret=params["client_secret"])
            if ozon.auth is None:
                return {'error': 'Ошибка авторизации'}
            else:
                if params["campaign_id"] is not None:
                    return {'result': ozon.get_objects(campaign_id=params["campaign_id"])}
                else:
                    return {'error': 'Не задан обязательный параметр campaign_id'}

        if method == 'available':
            # доступные режимы создания рекламных кампаний
            parser = reqparse.RequestParser()
            parser.add_argument("client_id")
            parser.add_argument("client_secret")
            params = parser.parse_args()
            ozon = OzonPerformance(client_id=params["client_id"],
                                   client_secret=params["client_secret"])
            if ozon.auth is None:
                return {'error': 'Ошибка авторизации'}
            else:
                res = ozon.get_camp_modes()
                try:
                    if res.status_code == 200:
                        return {'result': res.json()}
                    else:
                        return {'error': res.text, 'status_code': res.status_code}
                except:
                    return {'error': 'ошибка при обращении к серверу OZON'}

        if method == 'addcamp':
            # создать кампанию
            parser = reqparse.RequestParser()
            parser.add_argument("client_id", required=True)
            parser.add_argument("client_secret", required=True)
            parser.add_argument("title")
            parser.add_argument("from_date")
            parser.add_argument("to_date")
            parser.add_argument("daily_budget")
            parser.add_argument("exp_strategy")
            parser.add_argument("placement", required=True)
            parser.add_argument("product_autopilot_strategy")
            parser.add_argument("autopilot")
            parser.add_argument("pcm")
            params = parser.parse_args()
            print(params)
            ozon = OzonPerformance(client_id=params["client_id"],
                                   client_secret=params["client_secret"])
            if ozon.auth is None:
                return {'error': 'Ошибка авторизации'}
            else:
                if params["placement"] is not None:
                    res = ozon.create_camp2(title=params["title"],
                                            from_date=params["from_date"],
                                            to_date=params["to_date"],
                                            daily_budget=params["daily_budget"],
                                            exp_strategy=params["exp_strategy"],
                                            placement=params["placement"],
                                            product_autopilot_strategy=params["product_autopilot_strategy"],
                                            autopilot=params["autopilot"],
                                            pcm=params["pcm"])
                    try:
                        if res.status_code == 200:
                            return {'result': res.json(), 'message': 'Кампания создана'}
                        else:
                            return {'error': res.text, 'message': 'Ошибка запроса', 'status_code': res.status_code}
                    except:
                        return {'error': 'ошибка при обращении к серверу OZON'}
                else:
                    return {'error': 'Не задан обязательный параметр placement'}

        if method == 'activate':
            # активировать кампанию
            parser = reqparse.RequestParser()
            parser.add_argument("client_id", required=True)
            parser.add_argument("client_secret", required=True)
            parser.add_argument("campaign_id", required=True)
            params = parser.parse_args()
            ozon = OzonPerformance(client_id=params["client_id"],
                                   client_secret=params["client_secret"])
            if ozon.auth is None:
                return {'error': 'Ошибка авторизации'}
            else:
                res = ozon.camp_activate(campaign_id=params["campaign_id"])
                try:
                    if res.status_code == 200:
                        return {'result': res.json(), 'message': 'Кампания активирована'}
                    else:
                        return {'error': res.text, 'message': 'Ошибка запроса', 'status_code': res.status_code}
                except:
                    return {'error': 'ошибка при обращении к серверу OZON'}

        if method == 'deactivate':
            # деактивировать кампанию
            parser = reqparse.RequestParser()
            parser.add_argument("client_id", required=True)
            parser.add_argument("client_secret", required=True)
            parser.add_argument("campaign_id", required=True)
            params = parser.parse_args()
            ozon = OzonPerformance(client_id=params["client_id"],
                                   client_secret=params["client_secret"])
            if ozon.auth is None:
                return {'error': 'Ошибка авторизации'}
            else:
                res = ozon.camp_deactivate(campaign_id=params["campaign_id"])
                try:
                    if res.status_code == 200:
                        return {'result': res.json(), 'message': 'Кампания деактивирована'}
                    else:
                        return {'error': res.text, 'message': 'Ошибка запроса', 'status_code': res.status_code}
                except:
                    return {'error': 'ошибка при обращении к серверу OZON'}

        if method == 'period':
            # изменить сроки проведения кампании
            parser = reqparse.RequestParser()
            parser.add_argument("client_id", required=True)
            parser.add_argument("client_secret", required=True)
            parser.add_argument("campaign_id", required=True)
            parser.add_argument("date_from")
            parser.add_argument("date_to")
            params = parser.parse_args()
            ozon = OzonPerformance(client_id=params["client_id"],
                                   client_secret=params["client_secret"])
            if ozon.auth is None:
                return {'error': 'Ошибка авторизации'}
            else:
                if params["campaign_id"] is not None:
                    res = ozon.camp_period(campaign_id=params["campaign_id"],
                                           date_from=params["date_from"],
                                           date_to=params["date_to"])
                    try:
                        if res.status_code == 200:
                            return {'result': res.json(), 'message': 'Сроки изменены'}
                        else:
                            return {'error': res.text, 'message': 'Ошибка запроса', 'status_code': res.status_code}
                    except:
                        return {'error': 'ошибка при обращении к серверу OZON'}
                else:
                    return {'error': 'Не задан обязательный параметр campaign_id'}

        if method == 'budget':
            # изменить ограничения дневного бюджета кампании
            parser = reqparse.RequestParser()
            parser.add_argument("client_id", required=True)
            parser.add_argument("client_secret", required=True)
            parser.add_argument("campaign_id", required=True)
            parser.add_argument("daily_budget")
            parser.add_argument("exp_str")
            params = parser.parse_args()
            ozon = OzonPerformance(client_id=params["client_id"],
                                   client_secret=params["client_secret"])
            if ozon.auth is None:
                return {'error': 'Ошибка авторизации'}
            else:
                if params["campaign_id"] is not None:
                    res = ozon.camp_budget(campaign_id=params["campaign_id"],
                                           daily_budget=params["daily_budget"],
                                           exp_str=params["exp_str"])
                    try:
                        if res.status_code == 200:
                            return {'result': res.json(), 'message': 'Дневной бюджет изменен'}
                        else:
                            return {'error': res.text, 'message': 'Ошибка запроса', 'status_code': res.status_code}
                    except:
                        return {'error': 'ошибка при обращении к серверу OZON'}
                else:
                    return {'error': 'Не задан обязательный параметр campaign_id'}

        if method == 'addgroup':
            # создать группу
            parser = reqparse.RequestParser()
            parser.add_argument("client_id", required=True)
            parser.add_argument("client_secret", required=True)
            parser.add_argument("campaign_id", required=True)
            parser.add_argument("title")
            parser.add_argument("stopwords", action='append')
            parser.add_argument("phrases", action='append')
            parser.add_argument("bids_list", type=float, action='append')
            params = parser.parse_args()
            ozon = OzonPerformance(client_id=params["client_id"],
                                   client_secret=params["client_secret"])
            if ozon.auth is None:
                return {'error': 'Ошибка авторизации'}
            else:
                if params["campaign_id"] is not None:
                    res = ozon.add_group(campaign_id=params["campaign_id"],
                                         title=params["title"],
                                         stopwords=params["stopwords"],
                                         phrases=params["phrases"],
                                         bids_list=params["bids_list"])
                    try:
                        if res.status_code == 200:
                            return {'result': res.json(), 'message': 'Группа создана'}
                        else:
                            return {'error': res.text, 'message': 'Ошибка запроса', 'status_code': res.status_code}
                    except:
                        return {'error': 'ошибка при обращении к серверу OZON'}
                else:
                    return {'error': 'Не задан обязательный параметр campaign_id'}

        if method == 'editgroup':
            # редактировать группу
            parser = reqparse.RequestParser()
            parser.add_argument("client_id", required=True)
            parser.add_argument("client_secret", required=True)
            parser.add_argument("campaign_id", required=True)
            parser.add_argument("group_id")
            parser.add_argument("title")
            parser.add_argument("stopwords", action='append')
            parser.add_argument("phrases", action='append')
            parser.add_argument("bids_list", type=float, action='append')
            params = parser.parse_args()
            ozon = OzonPerformance(client_id=params["client_id"],
                                   client_secret=params["client_secret"])
            if ozon.auth is None:
                return {'error': 'Ошибка авторизации'}
            else:
                if params["campaign_id"] is not None and params["group_id"] is not None:
                    res = ozon.edit_group(campaign_id=params["campaign_id"],
                                          group_id=params["group_id"],
                                          title=params["title"],
                                          stopwords=params["stopwords"],
                                          phrases=params["phrases"],
                                          bids_list=params["bids_list"])
                    try:
                        if res.status_code == 200:
                            return {'result': res.json(), 'message': 'Группа изменена'}
                        else:
                            return {'error': res.text, 'message': 'Ошибка запроса', 'status_code': res.status_code}
                    except:
                        return {'error': 'ошибка при обращении к серверу OZON'}
                else:
                    return {'error': 'Не задан обязательный параметр campaign_id или group_id'}

        if method == 'addcardproducts':
            # добавить товары в кампанию с размещением в карточке товара
            parser = reqparse.RequestParser()
            parser.add_argument("client_id", required=True)
            parser.add_argument("client_secret", required=True)
            parser.add_argument("campaign_id", required=True)
            parser.add_argument("sku_list", action='append')
            parser.add_argument("bids_list", type=float, action='append')
            params = parser.parse_args()
            ozon = OzonPerformance(client_id=params["client_id"],
                                   client_secret=params["client_secret"])
            if ozon.auth is None:
                return {'error': 'Ошибка авторизации'}
            else:
                if params["campaign_id"] is not None:
                    bids = ozon.card_bids(sku_list=params["sku_list"],
                                          bids_list=params["bids_list"])
                    if bids is not None:
                        res = ozon.add_products(campaign_id=params["campaign_id"], bids=bids)
                        try:
                            if res.status_code == 200:
                                return {'result': res.json(), 'message': 'Добавлено'}
                            else:
                                return {'error': res.text, 'message': 'Ошибка при обращении к серверу OZON',
                                        'status_code': res.status_code}
                        except:
                            return {'error': 'Не известная ошибка при обращении к серверу OZON'}
                    else:
                        return {'error': 'Не правильный формат данных'}
                else:
                    return {'error': 'Не задан обязательный параметр campaign_id'}

        if method == 'addgroupproducts':
            # добавление в кампанию товаров в ранее созданные группы с размещением на страницах каталога и поиска
            parser = reqparse.RequestParser()
            parser.add_argument("client_id", required=True)
            parser.add_argument("client_secret", required=True)
            parser.add_argument("campaign_id", required=True)
            parser.add_argument("sku_list", action='append')
            parser.add_argument("bids_list", type=float, action='append')
            parser.add_argument("groups_list", type=str, action='append')
            params = parser.parse_args()
            ozon = OzonPerformance(client_id=params["client_id"],
                                   client_secret=params["client_secret"])
            if ozon.auth is None:
                return {'error': 'Ошибка авторизации'}
            else:
                if params["campaign_id"] is not None:
                    bids = ozon.group_bids(sku_list=params["sku_list"],
                                           bids_list=params["bids_list"],
                                           groups_list=params["groups_list"])
                    # print(bids)
                    if bids is not None:
                        res = ozon.add_products(campaign_id=params["campaign_id"], bids=bids)
                        try:
                            if res.status_code == 200:
                                return {'result': res.json(), 'message': 'Добавлено'}
                            else:
                                return {'error': res.text, 'message': 'Ошибка при обращении к серверу OZON',
                                        'status_code': res.status_code}
                        except:
                            return {'error': 'Не известная ошибка при обращении к серверу OZON'}
                    else:
                        return {'error': 'Не правильный формат данных'}
                else:
                    return {'error': 'Не задан обязательный параметр campaign_id'}

        if method == 'addproduct':
            # добавление товара на страницах каталога и поиска — добавление без группы
            parser = reqparse.RequestParser()
            parser.add_argument("client_id", required=True)
            parser.add_argument("client_secret", required=True)
            parser.add_argument("campaign_id", required=True)
            parser.add_argument("sku")
            parser.add_argument("stopwords", action='append')
            parser.add_argument("phrases", action='append')
            parser.add_argument("bids_list", type=float, action='append')
            params = parser.parse_args()
            ozon = OzonPerformance(client_id=params["client_id"],
                                   client_secret=params["client_secret"])
            if ozon.auth is None:
                return {'error': 'Ошибка авторизации'}
            else:
                if params["campaign_id"] is not None:
                    bids = ozon.phrases_bid(sku=params["sku"],
                                            stopwords=params["stopwords"],
                                            phrases=params["phrases"],
                                            bids_list=params["bids_list"])
                    if bids is not None:
                        res = ozon.add_products(campaign_id=params["campaign_id"], bids=bids)
                        try:
                            if res.status_code == 200:
                                return {'result': res.json(), 'message': 'Добавлено'}
                            else:
                                return {'error': res.text, 'message': 'Ошибка при обращении к серверу OZON',
                                        'status_code': res.status_code}
                        except:
                            return {'error': 'Не известная ошибка при обращении к серверу OZON'}
                    else:
                        return {'error': 'Не правильный формат данных'}
                else:
                    return {'error': 'Не задан обязательный параметр campaign_id'}

        if method == 'updbidscardproducts':
            # обновление ставок товаров с размещением в карточке товара
            parser = reqparse.RequestParser()
            parser.add_argument("client_id", required=True)
            parser.add_argument("client_secret", required=True)
            parser.add_argument("campaign_id", required=True)
            parser.add_argument("sku_list", action='append')
            parser.add_argument("bids_list", type=float, action='append')
            params = parser.parse_args()
            ozon = OzonPerformance(client_id=params["client_id"],
                                   client_secret=params["client_secret"])
            if ozon.auth is None:
                return {'error': 'Ошибка авторизации'}
            else:
                if params["campaign_id"] is not None:
                    bids = ozon.card_bids(sku_list=params["sku_list"],
                                          bids_list=params["bids_list"])
                    if bids is not None:
                        res = ozon.upd_bids(campaign_id=params["campaign_id"], bids=bids)
                        try:
                            if res.status_code == 200:
                                return {'result': res.json(), 'message': 'Обновлено'}
                            else:
                                return {'error': res.text, 'message': 'Ошибка при обращении к серверу OZON',
                                        'status_code': res.status_code}
                        except:
                            return {'error': 'Не известная ошибка при обращении к серверу OZON'}
                    else:
                        return {'error': 'Не правильный формат данных'}
                else:
                    return {'error': 'Не задан обязательный параметр campaign_id'}

        if method == 'updbidsgroupproducts':
            # обновление ставок товаров в группах с размещением на страницах каталога и поиска
            parser = reqparse.RequestParser()
            parser.add_argument("client_id", required=True)
            parser.add_argument("client_secret", required=True)
            parser.add_argument("campaign_id", required=True)
            parser.add_argument("sku_list", action='append')
            parser.add_argument("bids_list", type=float, action='append')
            parser.add_argument("groups_list", type=str, action='append')
            params = parser.parse_args()
            ozon = OzonPerformance(client_id=params["client_id"],
                                   client_secret=params["client_secret"])
            if ozon.auth is None:
                return {'error': 'Ошибка авторизации'}
            else:
                if params["campaign_id"] is not None:
                    bids = ozon.group_bids(sku_list=params["sku_list"],
                                           bids_list=params["bids_list"],
                                           groups_list=params["groups_list"])
                    if bids is not None:
                        res = ozon.upd_bids(campaign_id=params["campaign_id"], bids=bids)
                        try:
                            if res.status_code == 200:
                                return {'result': 'Обновлено'}
                            else:
                                return {'error': 'Ошибка при обращении к серверу OZON', 'status_code': res.status_code}
                        except:
                            return {'error': 'Не известная ошибка при обращении к серверу OZON'}
                    else:
                        return {'error': 'Не правильный формат данных'}
                else:
                    return {'error': 'Не задан обязательный параметр campaign_id'}

        if method == 'updbidsproducts':
            # обновление ставок товара на страницах каталога и поиска — без группы
            parser = reqparse.RequestParser()
            parser.add_argument("client_id", required=True)
            parser.add_argument("client_secret", required=True)
            parser.add_argument("campaign_id", required=True)
            parser.add_argument("sku")
            parser.add_argument("stopwords", action='append')
            parser.add_argument("phrases", action='append')
            parser.add_argument("bids_list", type=float, action='append')
            params = parser.parse_args()
            ozon = OzonPerformance(client_id=params["client_id"],
                                   client_secret=params["client_secret"])
            if ozon.auth is None:
                return {'error': 'Ошибка авторизации'}
            else:
                if params["campaign_id"] is not None:
                    bids = ozon.phrases_bid(sku=params["sku"],
                                            stopwords=params["stopwords"],
                                            phrases=params["phrases"],
                                            bids_list=params["bids_list"])
                    if bids is not None:
                        res = ozon.upd_bids(campaign_id=params["campaign_id"], bids=bids)
                        try:
                            if res.status_code == 200:
                                return {'result': res.json(), 'message': 'Обновлено'}
                            else:
                                return {'error': res.text, 'message': 'Ошибка при обращении к серверу OZON',
                                        'status_code': res.status_code}
                        except:
                            return {'error': 'Не известная ошибка при обращении к серверу OZON'}
                    else:
                        return {'error': 'Не правильный формат данных'}
                else:
                    return {'error': 'Не задан обязательный параметр campaign_id'}

        if method == 'prodlist':
            # список товаров кампании
            parser = reqparse.RequestParser()
            parser.add_argument("client_id", required=True)
            parser.add_argument("client_secret", required=True)
            parser.add_argument("campaign_id", required=True)
            params = parser.parse_args()
            ozon = OzonPerformance(client_id=params["client_id"],
                                   client_secret=params["client_secret"])
            if ozon.auth is None:
                return {'error': 'Ошибка авторизации'}
            else:
                if params["campaign_id"] is not None:
                    res = ozon.prod_list(campaign_id=params["campaign_id"])
                    try:
                        if res.status_code == 200:
                            return {'result': res.json()}
                        else:
                            return {'error': res.text, 'message': 'Ошибка запроса',
                                    'status_code': res.status_code}
                    except:
                        return {'error': 'Ошибка при обращении к серверу OZON'}
                else:
                    return {'error': 'Не задан обязательный параметр campaign_id'}

        if method == 'delproducts':
            # удалить товары из кампании
            parser = reqparse.RequestParser()
            parser.add_argument("client_id", required=True)
            parser.add_argument("client_secret", required=True)
            parser.add_argument("campaign_id", required=True)
            parser.add_argument("sku_list", action='append')
            params = parser.parse_args()
            ozon = OzonPerformance(client_id=params["client_id"],
                                   client_secret=params["client_secret"])
            if ozon.auth is None:
                return {'error': 'Ошибка авторизации'}
            else:
                if params["campaign_id"] is not None:
                    res = ozon.del_products(campaign_id=params["campaign_id"], sku_list=params["sku_list"])
                    try:
                        if res.status_code == 200:
                            return {'result': res.json(), 'message': 'Удалено'}
                        else:
                            return {'error': res.text, 'message': 'Ошибка при обращении к серверу OZON',
                                    'status_code': res.status_code}
                    except:
                        return {'error': 'Не известная ошибка при обращении к серверу OZON'}
                else:
                    return {'error': 'Не задан обязательный параметр campaign_id'}


api.add_resource(Main, "/ozonperformance/<string:method>")
api.init_app(app)

if __name__ == '__main__':
    # app.run(debug=True, host='api.ecomru.ru', port=5000)
    # app.run(debug=True, port=3000, host="127.0.0.1")
    app.run(debug=False, port=port, host=host)
