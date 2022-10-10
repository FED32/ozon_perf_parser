from flask import Flask, jsonify, request
from werkzeug.exceptions import BadRequestKeyError
from configparser import ConfigParser
from ozon_performance import OzonPerformance
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
# template = {"swagger": "2.0",
#             "info": {"title": "ECOMSELLER OZON Performance API",
#                      "description": "API для взаимодействия с кабинетом OZON Performance",
#                      "version": "0.1.0",
#                      "contact": {"name": "sfedyushin",
#                                  "url": "https://ecomru.ru/"}
#                      }
#             }

app.config['SWAGGER'] = {'title': 'API for OZON Performance',
                         'uiversion': 3,
                         "specs_route": "/apidocs2/"}

swagger = Swagger(app)
app.config.from_object(config.Config)

# список кампаний
@app.route('/ozonperformance/campaigns', methods=['POST'])
def get_campaigns():
    """
    Метод для вывода списка кампаний
        ---
        parameters:
          - name: body
            in: body
            type: object
            required: true
        definitions:
          Client Id:
            type: string
          Client Secret:
            type: string
          Campaigns:
            type: object

        responses:
          200:
            description: Список кампаний

    """
    try:
        json_file = request.get_json(force=False)
        print('aaaa', json_file)
        ozon = OzonPerformance(client_id=json_file["client_id"],
                               client_secret=json_file["client_secret"])
        if ozon.auth is None:
            return jsonify({'error': 'Ошибка авторизации'})
        else:
            return jsonify({'result': ozon.get_campaigns()})
    except BadRequestKeyError:
        return Response("Пустое значение", 400)

# список рекламируемых объектов в кампании
@app.route('/ozonperformance/objects', methods=['POST'])
def get_objects():
    try:
        json_file = request.get_json(force=False)
        ozon = OzonPerformance(client_id=json_file["client_id"],
                               client_secret=json_file["client_secret"])
        if ozon.auth is None:
            return jsonify({'error': 'Ошибка авторизации'})
        else:
            if json_file["campaign_id"] is not None:
                return jsonify({'result': ozon.get_objects(campaign_id=json_file["campaign_id"])})
            else:
                return jsonify({'error': 'Не задан обязательный параметр campaign_id'})
    except BadRequestKeyError:
        return Response("Пустое значение", 400)

# доступные режимы создания рекламных кампаний
@app.route('/ozonperformance/available', methods=['POST'])
def get_obj():
    try:
        json_file = request.get_json(force=False)
        ozon = OzonPerformance(client_id=json_file["client_id"],
                               client_secret=json_file["client_secret"])
        if ozon.auth is None:
            return jsonify({'error': 'Ошибка авторизации'})
        else:
            res = ozon.get_camp_modes()
            try:
                if res.status_code == 200:
                    return jsonify({'result': res.json()})
                else:
                    return jsonify({'error': res.text, 'status_code': res.status_code})
            except:
                return jsonify({'error': 'ошибка при обращении к серверу OZON'})
    except BadRequestKeyError:
        return Response("Пустое значение", 400)

# создать кампанию
@app.route('/ozonperformance/addcamp', methods=['POST'])
def add_campaign():
    try:
        json_file = request.get_json(force=False)
        ozon = OzonPerformance(client_id=json_file["client_id"],
                               client_secret=json_file["client_secret"])
        if ozon.auth is None:
            return jsonify({'error': 'Ошибка авторизации'})
        else:
            res = ozon.create_camp2(title=json_file["title"],
                                    from_date=json_file["from_date"],
                                    to_date=json_file["to_date"],
                                    daily_budget=json_file["daily_budget"],
                                    exp_strategy=json_file["exp_strategy"],
                                    placement=json_file["placement"],
                                    product_autopilot_strategy=json_file["product_autopilot_strategy"],
                                    autopilot=json_file["autopilot"],
                                    pcm=json_file["pcm"])
            try:
                if res.status_code == 200:
                    return jsonify({'result': res.json(), 'message': 'Кампания создана'})
                else:
                    return jsonify({'error': res.text, 'message': 'Ошибка запроса', 'status_code': res.status_code})
            except:
                return jsonify({'error': 'ошибка при обращении к серверу OZON'})
    except BadRequestKeyError:
        return Response("Пустое значение", 400)

# активировать кампанию
@app.route('/ozonperformance/activate', methods=['POST'])
def activate_camp():
    try:
        json_file = request.get_json(force=False)
        ozon = OzonPerformance(client_id=json_file["client_id"],
                               client_secret=json_file["client_secret"])
        if ozon.auth is None:
            return jsonify({'error': 'Ошибка авторизации'})
        else:
            res = ozon.camp_activate(campaign_id=json_file["campaign_id"])
            try:
                if res.status_code == 200:
                    return jsonify({'result': res.json(), 'message': 'Кампания активирована'})
                else:
                    return jsonify({'error': res.text, 'message': 'Ошибка запроса', 'status_code': res.status_code})
            except:
                return jsonify({'error': 'ошибка при обращении к серверу OZON'})
    except BadRequestKeyError:
        return Response("Пустое значение", 400)

# деактивировать кампанию
@app.route('/ozonperformance/deactivate', methods=['POST'])
def deactivate_camp():
    try:
        json_file = request.get_json(force=False)
        ozon = OzonPerformance(client_id=json_file["client_id"],
                               client_secret=json_file["client_secret"])
        if ozon.auth is None:
            return jsonify({'error': 'Ошибка авторизации'})
        else:
            res = ozon.camp_deactivate(campaign_id=json_file["campaign_id"])
            try:
                if res.status_code == 200:
                    return jsonify({'result': res.json(), 'message': 'Кампания деактивирована'})
                else:
                    return jsonify({'error': res.text, 'message': 'Ошибка запроса', 'status_code': res.status_code})
            except:
                return jsonify({'error': 'ошибка при обращении к серверу OZON'})
    except BadRequestKeyError:
        return Response("Пустое значение", 400)

# изменить сроки проведения кампании
@app.route('/ozonperformance/period', methods=['POST'])
def campaign_period():
    try:
        json_file = request.get_json(force=False)
        ozon = OzonPerformance(client_id=json_file["client_id"],
                               client_secret=json_file["client_secret"])
        if ozon.auth is None:
            return jsonify({'error': 'Ошибка авторизации'})
        else:
            res = ozon.camp_period(campaign_id=json_file["campaign_id"],
                                   date_from=json_file["date_from"],
                                   date_to=json_file["date_to"])
            try:
                if res.status_code == 200:
                    return jsonify({'result': res.json(), 'message': 'Сроки изменены'})
                else:
                    return jsonify({'error': res.text, 'message': 'Ошибка запроса', 'status_code': res.status_code})
            except:
                return jsonify({'error': 'ошибка при обращении к серверу OZON'})
    except BadRequestKeyError:
        return Response("Пустое значение", 400)

# изменить ограничения дневного бюджета кампании
@app.route('/ozonperformance/budget', methods=['POST'])
def campaign_budget():
    try:
        json_file = request.get_json(force=False)
        ozon = OzonPerformance(client_id=json_file["client_id"],
                               client_secret=json_file["client_secret"])
        if ozon.auth is None:
            return jsonify({'error': 'Ошибка авторизации'})
        else:
            res = ozon.camp_budget(campaign_id=json_file["campaign_id"],
                                   daily_budget=float(json_file["daily_budget"]),
                                   exp_str=json_file["exp_str"])
            try:
                if res.status_code == 200:
                    return jsonify({'result': res.json(), 'message': 'Дневной бюджет изменен'})
                else:
                    return jsonify({'error': res.text, 'message': 'Ошибка запроса', 'status_code': res.status_code})
            except:
                return jsonify({'error': 'ошибка при обращении к серверу OZON'})
    except BadRequestKeyError:
        return Response("Пустое значение", 400)

# создать группу
@app.route('/ozonperformance/addgroup', methods=['POST'])
def add_group():
    try:
        json_file = request.get_json(force=False)
        ozon = OzonPerformance(client_id=json_file["client_id"],
                               client_secret=json_file["client_secret"])
        if ozon.auth is None:
            return jsonify({'error': 'Ошибка авторизации'})
        else:
            res = ozon.add_group(campaign_id=json_file["campaign_id"],
                                 title=json_file["title"],
                                 stopwords=json_file["stopwords"],
                                 phrases=json_file["phrases"],
                                 bids_list=json_file["bids_list"])
            try:
                if res.status_code == 200:
                    return jsonify({'result': res.json(), 'message': 'Группа создана'})
                else:
                    return jsonify({'error': res.text, 'message': 'Ошибка запроса', 'status_code': res.status_code})
            except:
                return jsonify({'error': 'ошибка при обращении к серверу OZON'})
    except BadRequestKeyError:
        return Response("Пустое значение", 400)

# редактировать группу
@app.route('/ozonperformance/editgroup', methods=['POST'])
def edit_group():
    try:
        json_file = request.get_json(force=False)
        ozon = OzonPerformance(client_id=json_file["client_id"],
                               client_secret=json_file["client_secret"])
        if ozon.auth is None:
            return jsonify({'error': 'Ошибка авторизации'})
        else:
            res = ozon.edit_group(campaign_id=json_file["campaign_id"],
                                  group_id=json_file["group_id"],
                                  title=json_file["title"],
                                  stopwords=json_file["stopwords"],
                                  phrases=json_file["phrases"],
                                  bids_list=json_file["bids_list"])
            try:
                if res.status_code == 200:
                    return jsonify({'result': res.json(), 'message': 'Группа изменена'})
                else:
                    return jsonify({'error': res.text, 'message': 'Ошибка запроса', 'status_code': res.status_code})
            except:
                return jsonify({'error': 'ошибка при обращении к серверу OZON'})
    except BadRequestKeyError:
        return Response("Пустое значение", 400)

# добавить товары в кампанию с размещением в карточке товара
@app.route('/ozonperformance/addcardproducts', methods=['POST'])
def addcardproducts():
    try:
        json_file = request.get_json(force=False)
        ozon = OzonPerformance(client_id=json_file["client_id"],
                               client_secret=json_file["client_secret"])
        if ozon.auth is None:
            return jsonify({'error': 'Ошибка авторизации'})
        else:
            bids = ozon.card_bids(sku_list=json_file["sku_list"],
                                  bids_list=json_file["bids_list"])
            if bids is not None:
                res = ozon.add_products(campaign_id=json_file["campaign_id"], bids=bids)
                try:
                    if res.status_code == 200:
                        return jsonify({'result': res.json(), 'message': 'Добавлено'})
                    else:
                        return jsonify({'error': res.text, 'message': 'Ошибка при обращении к серверу OZON',
                                        'status_code': res.status_code})
                except:
                    return jsonify({'error': 'Не известная ошибка при обращении к серверу OZON'})
            else:
                return jsonify({'error': 'Не правильный формат данных'})
    except BadRequestKeyError:
        return Response("Пустое значение", 400)

# добавление в кампанию товаров в ранее созданные группы с размещением на страницах каталога и поиска
@app.route('/ozonperformance/addgroupproducts', methods=['POST'])
def addgroupproducts():
    try:
        json_file = request.get_json(force=False)
        ozon = OzonPerformance(client_id=json_file["client_id"],
                               client_secret=json_file["client_secret"])
        if ozon.auth is None:
            return jsonify({'error': 'Ошибка авторизации'})
        else:
            bids = ozon.group_bids(sku_list=json_file["sku_list"],
                                   bids_list=json_file["bids_list"],
                                   groups_list=json_file["groups_list"])
            if bids is not None:
                res = ozon.add_products(campaign_id=json_file["campaign_id"], bids=bids)
                try:
                    if res.status_code == 200:
                        return jsonify({'result': res.json(), 'message': 'Добавлено'})
                    else:
                        return jsonify({'error': res.text, 'message': 'Ошибка при обращении к серверу OZON',
                                        'status_code': res.status_code})
                except:
                    return jsonify({'error': 'Не известная ошибка при обращении к серверу OZON'})
            else:
                return jsonify({'error': 'Не правильный формат данных'})
    except BadRequestKeyError:
        return Response("Пустое значение", 400)

# добавление товара на страницах каталога и поиска — добавление без группы
@app.route('/ozonperformance/addproduct', methods=['POST'])
def addproduct():
    try:
        json_file = request.get_json(force=False)
        ozon = OzonPerformance(client_id=json_file["client_id"],
                               client_secret=json_file["client_secret"])
        if ozon.auth is None:
            return jsonify({'error': 'Ошибка авторизации'})
        else:
            bids = ozon.phrases_bid(sku=json_file["sku"],
                                    stopwords=json_file["stopwords"],
                                    phrases=json_file["phrases"],
                                    bids_list=json_file["bids_list"])
            if bids is not None:
                res = ozon.add_products(campaign_id=json_file["campaign_id"], bids=bids)
                try:
                    if res.status_code == 200:
                        return jsonify({'result': res.json(), 'message': 'Добавлено'})
                    else:
                        return jsonify({'error': res.text, 'message': 'Ошибка при обращении к серверу OZON',
                                        'status_code': res.status_code})
                except:
                    return jsonify({'error': 'Не известная ошибка при обращении к серверу OZON'})
            else:
                return jsonify({'error': 'Не правильный формат данных'})
    except BadRequestKeyError:
        return Response("Пустое значение", 400)

# обновление ставок товаров с размещением в карточке товара
@app.route('/ozonperformance/updbidscardproducts', methods=['POST'])
def updbidscardproducts():
    try:
        json_file = request.get_json(force=False)
        ozon = OzonPerformance(client_id=json_file["client_id"],
                               client_secret=json_file["client_secret"])
        if ozon.auth is None:
            return jsonify({'error': 'Ошибка авторизации'})
        else:
            bids = ozon.card_bids(sku_list=json_file["sku_list"],
                                  bids_list=json_file["bids_list"])
            if bids is not None:
                res = ozon.upd_bids(campaign_id=json_file["campaign_id"], bids=bids)
                try:
                    if res.status_code == 200:
                        return jsonify({'result': res.json(), 'message': 'Обновлено'})
                    else:
                        return jsonify({'error': res.text, 'message': 'Ошибка при обращении к серверу OZON',
                                        'status_code': res.status_code})
                except:
                    return jsonify({'error': 'Не известная ошибка при обращении к серверу OZON'})
            else:
                return jsonify({'error': 'Не правильный формат данных'})
    except BadRequestKeyError:
        return Response("Пустое значение", 400)

# обновление ставок товаров в группах с размещением на страницах каталога и поиска
@app.route('/ozonperformance/updbidsgroupproducts', methods=['POST'])
def updbidsgroupproducts():
    try:
        json_file = request.get_json(force=False)
        ozon = OzonPerformance(client_id=json_file["client_id"],
                               client_secret=json_file["client_secret"])
        if ozon.auth is None:
            return jsonify({'error': 'Ошибка авторизации'})
        else:
            bids = ozon.group_bids(sku_list=json_file["sku_list"],
                                   bids_list=json_file["bids_list"],
                                   groups_list=json_file["groups_list"])
            if bids is not None:
                res = ozon.upd_bids(campaign_id=json_file["campaign_id"], bids=bids)
                try:
                    if res.status_code == 200:
                        return jsonify({'result': 'Обновлено'})
                    else:
                        return jsonify({'error': 'Ошибка при обращении к серверу OZON', 'status_code': res.status_code})
                except:
                    return jsonify({'error': 'Не известная ошибка при обращении к серверу OZON'})
            else:
                return jsonify({'error': 'Не правильный формат данных'})
    except BadRequestKeyError:
        return Response("Пустое значение", 400)

# обновление ставок товара на страницах каталога и поиска — без группы
@app.route('/ozonperformance/updbidsproducts', methods=['POST'])
def updbidsproducts():
    try:
        json_file = request.get_json(force=False)
        ozon = OzonPerformance(client_id=json_file["client_id"],
                               client_secret=json_file["client_secret"])
        if ozon.auth is None:
            return jsonify({'error': 'Ошибка авторизации'})
        else:
            bids = ozon.phrases_bid(sku=json_file["sku"],
                                    stopwords=json_file["stopwords"],
                                    phrases=json_file["phrases"],
                                    bids_list=json_file["bids_list"])
            if bids is not None:
                res = ozon.upd_bids(campaign_id=json_file["campaign_id"], bids=bids)
                try:
                    if res.status_code == 200:
                        return jsonify({'result': res.json(), 'message': 'Обновлено'})
                    else:
                        return jsonify({'error': res.text, 'message': 'Ошибка при обращении к серверу OZON',
                                        'status_code': res.status_code})
                except:
                    return jsonify({'error': 'Не известная ошибка при обращении к серверу OZON'})
            else:
                return jsonify({'error': 'Не правильный формат данных'})
    except BadRequestKeyError:
        return Response("Пустое значение", 400)

# список товаров кампании
@app.route('/ozonperformance/prodlist', methods=['POST'])
def prodlist():
    try:
        json_file = request.get_json(force=False)
        ozon = OzonPerformance(client_id=json_file["client_id"],
                               client_secret=json_file["client_secret"])
        if ozon.auth is None:
            return jsonify({'error': 'Ошибка авторизации'})
        else:
            res = ozon.prod_list(campaign_id=json_file["campaign_id"])
            try:
                if res.status_code == 200:
                    return jsonify({'result': res.json()})
                else:
                    return jsonify({'error': res.text, 'message': 'Ошибка запроса',
                                    'status_code': res.status_code})
            except:
                return jsonify({'error': 'Ошибка при обращении к серверу OZON'})
    except BadRequestKeyError:
        return Response("Пустое значение", 400)

# удалить товары из кампании
@app.route('/ozonperformance/delproducts', methods=['POST'])
def delproducts():
    try:
        json_file = request.get_json(force=False)
        ozon = OzonPerformance(client_id=json_file["client_id"],
                               client_secret=json_file["client_secret"])
        if ozon.auth is None:
            return jsonify({'error': 'Ошибка авторизации'})
        else:
            res = ozon.del_products(campaign_id=json_file["campaign_id"],
                                    sku_list=json_file["sku_list"])
            try:
                if res.status_code == 200:
                    return jsonify({'result': res.json(), 'message': 'Удалено'})
                else:
                    return jsonify({'error': res.text, 'message': 'Ошибка при обращении к серверу OZON',
                                    'status_code': res.status_code})
            except:
                return jsonify({'error': 'Не известная ошибка при обращении к серверу OZON'})
    except BadRequestKeyError:
        return Response("Пустое значение", 400)


if __name__ == '__main__':
    app.run(debug=True, port=port, host=host)
