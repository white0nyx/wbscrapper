import time
import requests
import json
import csv
from art import tprint
import sqlite3


class WBScrapper:
    HEADERS = {
        'Accept': '*/*',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',
        'Origin': 'https://www.wildberries.ru',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'cross-site',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
        'sec-ch-ua': '".Not/A)Brand";v="99", "Google Chrome";v="103", "Chromium";v="103"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"'
    }

    START_OF_LINK = 'https://catalog.wb.ru/catalog/'
    MIDDLE_OF_LINK = '/catalog?appType=1&couponsGeo=12,3,18,15,21&curr=rub&dest=-1029256,-102269,-2162196,-1257786&emp=0&lang=ru&locale=ru&page='
    FINISH_OF_LINK = '&pricemarginCoeff=1.0&reg=0&regions=68,64,83,4,38,80,33,70,82,86,75,30,69,22,66,31,40,1,48,71&sort=popular&spp=0&'

    def __init__(self, categories, crt_all_catalog=False, crt_db=False):
        """Инициализация объекта парсера"""

        response = requests.get('https://www.wildberries.ru/webapi/menu/main-menu-ru-ru.json',
                                headers=self.HEADERS).json()

        self.catalog = response
        self.categories = list(map(int, categories.split()))
        self.categories = [num - 1 for num in self.categories]
        self.crt_all_catalog = crt_all_catalog
        self.crt_db = crt_db

        if crt_all_catalog:
            with open('all_catalog.json', 'w', encoding='utf-8') as file:
                json.dump(response, file, indent=4, ensure_ascii=False)

        if crt_db:
            with sqlite3.connect('products_database.db') as con:
                cur = con.cursor()

                cur.execute("""DROP TABLE IF EXISTS products""")

                cur.execute("""CREATE TABLE IF NOT EXISTS products (
                            __sort INTEGER,
                            k_sort INTEGER,
                            time1 INTEGER,
                            time2 INTEGER,
                            id INTEGER,
                            root INTEGER,
                            kindId INTEGER,
                            subjectId INTEGER,
                            subjectParentId INTEGER,
                            parent_category TEXT,
                            child_category TEXT,
                            name TEXT,
                            brand TEXT,
                            brandId INTEGER,
                            siteBrandId INTEGER,
                            sale INTEGER,
                            priceU REAL,
                            salePriceU REAL,
                            pics INTEGER,
                            rating INTEGER,
                            feedbacks INTEGER,
                            panelPromoId INTEGER,                      
                            promoTextCat TEXT,
                            link TEXT
                            )""")

        with open('products.csv', 'w', encoding='utf-8-sig', newline='') as file:
            writer = csv.writer(file, delimiter=';')
            writer.writerow(('__sort', 'k_sort', 'time1', 'time2', 'id', 'root', 'kindId', 'subjectId',
                             'subjectParentId', 'parent_category', 'child_category', 'name', 'brand', 'brandId',
                             'siteBrandId', 'sale', 'priceU', 'salePriceU', 'pics', 'rating', 'feedbacks',
                             'panelPromoId', 'promoTextCat', 'link'))

    def get_data_from_page(self, url_request, parent_category, child_category):
        """Получение данных с одной страницы"""

        response = requests.get(url=url_request,
                                headers=self.HEADERS).json()

        catalog = response['data']['products']

        for item in catalog:
            __sort = item['__sort']
            k_sort = item['ksort']
            time1 = item['time1']
            time2 = item['time2']
            id = item['id']
            root = item['root']
            kindId = item['kindId']
            subjectId = item['subjectId']
            subjectParentId = item['subjectParentId']
            name = item['name']
            brand = item['brand']
            brandId = item['brandId']
            siteBrandId = item['siteBrandId']
            sale = item['sale']
            priceU = float(str(item['priceU'])[:-2] + '.' + str(item['priceU'])[-2:])
            salePriceU = float(str(item['salePriceU'])[:-2] + '.' + str(item['salePriceU'])[-2:])
            pics = item['pics']
            rating = item['rating']
            feedbacks = item['feedbacks']

            if 'panelPromoId' in item:
                panelPromoId = item['panelPromoId']
            else:
                panelPromoId = None

            if 'promoTextCat' in item:
                promoTextCat = item['promoTextCat']
            else:
                promoTextCat = None

            link = f"https://www.wildberries.ru/catalog/{id}/detail.aspx?targetUrl=GP"

            all_data = (__sort, k_sort, time1, time2, id, root, kindId, subjectId, subjectParentId, parent_category,
                        child_category, name, brand, brandId, siteBrandId, sale, priceU, salePriceU, pics, rating,
                        feedbacks, panelPromoId, promoTextCat, link)

            with open('products.csv', 'a', encoding='utf-8-sig', newline='') as file:
                writer = csv.writer(file, delimiter=';')
                writer.writerow(all_data)

            if self.crt_db:
                with sqlite3.connect('products_database.db') as con:
                    cur = con.cursor()
                    cur.execute(f"""INSERT INTO products VALUES ({str('?, '* 24)[:-2]})""", all_data)

    def get_child_categories(self):
        """Получение списка, содержащего две части для формирования ссылки для каждой подкатегории"""

        all_child_categories = []
        for category in self.categories:
            category_name = self.catalog[category]['name']
            for child in self.catalog[category]['childs']:
                all_child_categories.append(
                    {
                        'parent_category': category_name,
                        'child_category': child['name'],
                        'first_part': self.START_OF_LINK + child['shard'] + self.MIDDLE_OF_LINK,
                        'final_part': self.FINISH_OF_LINK + child['query']
                    }
                )
        return all_child_categories

    def get_data_from_pages(self, pages):
        for child_category in self.get_child_categories():
            for page in range(1, pages + 1):
                req_url = child_category['first_part'] + str(page) + child_category['final_part']

                self.get_data_from_page(req_url, child_category['parent_category'], child_category['child_category'])
            print(child_category['parent_category'], child_category['child_category'], 'is_ready')
            # time.sleep(2)


if __name__ == '__main__':
    tprint('WBScrapper')
    print('Добро пожаловать в программу для парсинга интернет магазина www.wildberries.ru\n')
    print('Выберите категории: (Запишите номера категорий через пробел)')
    print('1.  Женщинам                 11. Продукты                21. Алкоголь                31. Региональные товары')
    print('2.  Обувь                    12. Бытовая техника         22. Сад и дача              32. Вкусы России')
    print('3.  Детям                    13. Зоотовары               23. Здоровье')
    print('4.  Мужчинам                 14. Спорт                   24. Канцтовары')
    print('5.  Дом                      15. Автотовары              25. Цифровые товары')
    print('6.  Красота                  16. Книги                   26. Экспресс-доставка')
    print('7.  Аксессуары               17. Premium                 27. Акции')
    print('8.  Электроника              18. Ювелирные изделия       28. Авиабилеты')
    print('9.  Игрушки                  19. Для ремонта             29. Бренды')
    print('10. Товары для взрослых      20. Мебель                  30. Видеообзоры')

    user_choice = input().strip()
    pages = int(input('Укажите количество страниц, которое необходимо получить с каждой категории: '))

    wbs = WBScrapper(user_choice, crt_db=True)

    wbs.get_data_from_pages(pages)
