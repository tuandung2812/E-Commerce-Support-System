from logging import warning
from flask import Flask, render_template, request
from flask_restful import Resource, Api
from scrape.LazadaScraping import scrape_lazada, scrape_lazada_by_product
from scrape.sendo_scrape import scrape_sendo
from scrape.tiki_scrape import scrape_tiki_by_name, scrape_tiki_by_url
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from db import db
from models.device import DeviceModel
from security import api_required
import hashlib
import json
from datetime import date, datetime, timedelta
from urllib.parse import urlparse
import os

# CHROME_DRIVER_PATH = 'D:/chromedriver.exe'

app = Flask(__name__)
api = Api(app)
app.config['SQLALCHEMY_ECHO'] = True
app.config ['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///db.sqlite3'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db.init_app(app)


@app.route('/')
def home():
    return render_template('index.html')


@app.route('/doc')
def documentation():
    return render_template('documentation.html')


@app.route('/api_key',  methods = ['GET', 'POST'])
def api_key():
    if request.method == 'POST':
        device_name = request.form.get('device_name')
        if DeviceModel.find_by_name(device_name=device_name):
            return render_template('generate_key.html', warning="Device name already existed! Please choose another name.")

        new_device = DeviceModel(
            device_name=device_name,
        )
        new_device.save_to_db()
        api_key = new_device.device_key
        return render_template('generate_key.html', key=api_key, warning=None)
    return render_template('generate_key.html', key=None, warning=None)


def get_cache_path(type, query, site):
    cache_file_name = str(type)+'__'+str(query)+'__'+str(site)
    cache_file_name = str(int(hashlib.sha1(cache_file_name.encode('utf-8')).hexdigest(), 16) % (10 ** 32))+'.json'
    cache_path = os.path.join(os.getcwd(), 'cache', cache_file_name)
    return cache_path


def load_cache_path(cache_path, max_review, product_num):
    cache_exist = False
    result = []
    if os.path.exists(cache_path):
        with open(cache_path, encoding='utf-8') as f:
            cache = json.load(f)

        date_recorded = datetime.strptime(cache['date'], '%y %m %d')
        if (datetime.combine(date.today(), datetime.min.time()) - date_recorded) <= timedelta(days = 15):
            if max_review <= cache['maxreview'] and product_num <= cache['productnum']:
                cache_exist = True
                result = cache['result'][:product_num]
                for product in result:
                    product['reviews'] = product['reviews'][:max_review]

    return cache_exist, result


def get_url_alias(url):
    url_object = urlparse(url)
    return url_object.netloc + url_object.path


class GetReviewByProductName(Resource):
    @api_required
    def get(self):
        product_name = request.args.get('name')
        site = request.args.get('site')
        if site not in ['sendo', 'lazada', 'tiki', 'all']:
            return f"Argument value '{site}' is not supported", 400
        try:
            max_review = int(request.args.get('maxreview', 5)) # 5 is default max_review
        except ValueError:
            return "Please provide an integer for 'maxreview' parameter", 400
        if max_review > 10:
            return "Request failed, maxreview must <= 10", 400
        try:
            product_num = int(request.args.get('productnum', 3)) # 3 is default product num
        except ValueError:
            return "Please provide an integer for 'productnum' parameter", 400
        if product_num > 10:
            return "Request failed, productnum must <= 10", 400

        # Cache path is used for individual site, if site == 'all' check each site separately
        cache_path = get_cache_path('productname', product_name, site)
        tiki_cache_path = get_cache_path('productname', product_name, 'tiki')
        sendo_cache_path = get_cache_path('productname', product_name, 'sendo')
        lazada_cache_path = get_cache_path('productname', product_name, 'lazada')

        cache_exist = False
        if site != 'all':
            cache_exist, result = load_cache_path(cache_path, max_review, product_num)

        if cache_exist:
            return result
        else:
            chrome_options = Options()
            chrome_options.binary_location = os.environ.get("GOOGLE_CHROME_BIN")
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument('--disable-gpu')
            
            driver = webdriver.Chrome(os.environ.get("CHROMEDRIVER_PATH"), options=chrome_options)

            if site == 'sendo':
                result = scrape_sendo(driver=driver, input=product_name, max_review_num=max_review,
                                      product_num=product_num, verbose=True)
            elif site == 'lazada':
                result = scrape_lazada_by_product(driver=driver, input=product_name, max_page=product_num,
                                                  max_comment_per_page=max_review)['result']
            elif site == 'tiki':
                result = scrape_tiki_by_name(driver=driver, input=product_name, product_num=product_num,
                                             max_review_num=max_review)
            else:
                result = []

                sendo_cache_exist, sendo_result = load_cache_path(sendo_cache_path, max_review, product_num)
                if not sendo_cache_exist:
                    sendo_result = scrape_sendo(driver=driver, input=product_name, max_review_num=max_review,
                                      product_num=product_num, verbose=True)
                    sendo_cache = {
                        'date': datetime.strftime(date.today(), '%y %m %d'),
                        'result' : sendo_result, 'maxreview': max_review,
                        'productnum': product_num
                    }
                    with open(sendo_cache_path, 'w', encoding='utf8') as json_file:
                        json.dump(sendo_cache, json_file, ensure_ascii=False)
                result.extend(sendo_result)

                tiki_cache_exist, tiki_result = load_cache_path(tiki_cache_path, max_review, product_num)
                if not tiki_cache_exist:
                    tiki_result = scrape_tiki_by_name(driver=driver, input=product_name, product_num=product_num,
                                             max_review_num=max_review)
                    tiki_cache = {
                        'date': datetime.strftime(date.today(), '%y %m %d'),
                        'result' : tiki_result, 'maxreview': max_review,
                        'productnum': product_num
                    }
                    with open(tiki_cache_path, 'w', encoding='utf8') as json_file:
                        json.dump(tiki_cache, json_file, ensure_ascii=False)
                result.extend(tiki_result)

                lazada_cache_exist, lazada_result = load_cache_path(lazada_cache_path, max_review, product_num)
                if not lazada_cache_exist:
                    lazada_result = scrape_lazada_by_product(driver=driver, input=product_name, product_num=product_num,
                                             max_review_num=max_review)
                    lazada_result = lazada_result['result']
                    lazada_cache = {
                        'date': datetime.strftime(date.today(), '%y %m %d'),
                        'result' : lazada_result, 'maxreview': max_review,
                        'productnum': product_num
                    }
                    with open(lazada_cache_path, 'w', encoding='utf8') as json_file:
                        json.dump(lazada_cache, json_file, ensure_ascii=False)
                result.extend(lazada_result)

            driver.quit()

            # Only save cache of individual site not all sites
            if site != 'all':
                cache = {'date': datetime.strftime(date.today(), '%y %m %d'), 'result' : result, 'maxreview': max_review, 'productnum': product_num}
                with open(cache_path, 'w', encoding='utf8') as json_file:
                    json.dump(cache, json_file, ensure_ascii=False)

        return result


class GetReviewByURL(Resource):
    @api_required
    def get(self):
        url = request.args.get('url')
        site = request.args.get('site')
        if site not in ['sendo', 'lazada', 'tiki']:
            return f"Argument value '{site}' is not supported", 400
        try:
            max_review = int(request.args.get('maxreview', 5)) # 5 is default max_review
        except ValueError:
            return "Please provide an integer for 'maxreview' parameter", 400
        if max_review > 10:
            return "Request failed, maxreview must <= 10", 400

        url_alias = get_url_alias(url)
        cache_path = get_cache_path('', url_alias, '')
        cache_exist = False

        # Load cache if exist
        if os.path.exists(cache_path):
            with open(cache_path, encoding='utf-8') as f:
                cache = json.load(f)

            date_recorded = datetime.strptime(cache['date'], '%y %m %d')
            if (datetime.combine(date.today(), datetime.min.time()) - date_recorded) < timedelta(days = 15):
                if max_review <= cache['maxreview']:
                    result = cache['result']
                    result['reviews'] = cache['result']['reviews'][:max_review]
                    cache_exist = True
                    return result

        # Scrape if not exist
        if not cache_exist:
            chrome_options = Options()
            chrome_options.binary_location = os.environ.get("GOOGLE_CHROME_BIN")
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument('--disable-gpu')
            driver = webdriver.Chrome(os.environ.get("CHROMEDRIVER_PATH"), options=chrome_options)

            if site == 'sendo':
                result = scrape_sendo(driver=driver, input=url, max_review_num=max_review)
            elif site == 'lazada':
                result = scrape_lazada(driver=driver, url=url, max_comment=max_review)
            elif site == 'tiki':
                result = scrape_tiki_by_url(driver=driver, url=url, max_review_num=max_review)

            driver.quit()

            cache = {}
            cache['result'] = result.copy()
            cache['maxreview'] = max_review
            cache['date'] = datetime.strftime(date.today(), '%y %m %d')
            with open(cache_path, 'w', encoding='utf8') as json_file:
                json.dump(cache, json_file, ensure_ascii=False)

        return result


class AddDevice(Resource):
    def post(self):
        name = request.form['device_name']

        if DeviceModel.find_by_name(device_name = name):
            return {'message': f'A device with name {name} already exists.'}, 400

        new_device = DeviceModel(
            device_name=name,
        )
        new_device.save_to_db()

        return  {'api_key': new_device.device_key}, 201


api.add_resource(AddDevice, '/new_api_key')
api.add_resource(GetReviewByURL, '/review/byurl')
api.add_resource(GetReviewByProductName, '/review/byname')

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    app.run(debug=True)
