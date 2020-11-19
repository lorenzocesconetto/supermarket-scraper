import requests
from bs4 import BeautifulSoup as bs4
import pandas as pd
import unidecode
import threading
import nltk
import time
from utils import print_pbar

pd.set_option('display.max_rows', 300)


URLS = [
    'https://www.superpaguemenos.com.br/6929-congelados/?p={}',               # 1-Congelados
    'https://www.superpaguemenos.com.br/8510-cafe-da-manha/?p={}',          # 2-Cafe
    'https://www.superpaguemenos.com.br/6932-higiene-e-beleza/?p={}',       # 3-Higiene
    'https://www.superpaguemenos.com.br/6933-feira/?p={}',                  # 4-Hortifruti
                                                                            # 5-Carnes
    'https://www.superpaguemenos.com.br/8519-frios-e-laticinios/?p={}',     # 6-Frios e Lat
    'https://www.superpaguemenos.com.br/6935-limpeza/?p={}',                # 7-Limpeza
    'https://www.superpaguemenos.com.br/9616-utilidades-domesticas/?p={}',  # 8-Bazar
                                                                            # 9-Festivos
    'https://www.superpaguemenos.com.br/8295-mercearia/?p={}',              # 10-Mercearia
    'https://www.superpaguemenos.com.br/8382-bebidas/?p={}',                # 11-Bebidas
]


class PagueMenosScraper:
    def __init__(self, urls) -> None:
        self.urls = urls
        self.data = dict()
        self.stopwords = nltk.corpus.stopwords.words('portuguese')

    def _get_page(self, url):
        page = requests.get(url)
        return bs4(page.content, 'html.parser')

    def _get_num_pages(self, url) -> int:
        """Get number of pages in pagination"""
        soup = self._get_page(url)
        text = soup.find('li', {'class': 'info'}).get_text().strip()
        return int(text.split()[-1])

    def _get_ref(self, detail_page):
        """Get reference code"""
        return int(detail_page.find('span', {'itemprop': 'sku'}).get_text())

    def _get_brand(self, detail_page):
        """Get the brand of the item"""
        try:
            return detail_page.find('a', {'itemprop': 'brand'}).get_text()
        except:
            return False

    def _get_name(self, item):
        name = item.find('span', {'itemprop': 'name'}).get_text()
        name = name.strip().lower()
        name = unidecode.unidecode(name)
        return ' '.join([x for x in name.split() if x not in self.stopwords])

    def _get_price(self, item):
        """Get item price"""
        try:
            price = item.find('strong', {'class': 'price'})
            price = price.get_text()[3:].replace(',', '.')
            return float(price)
        except:
            return False

    def _get_sku(self, item):
        """Get item SKU"""
        return int(item['data-sku'])

    def _get_discount(self, item):
        """Get discount percentage, if not found, return zero"""
        discount = item.find('span', {'class': 'descont_percentage'})
        if discount:
            return int(discount.find('strong').get_text())
        return 0

    def _get_clube(self, item) -> bool:
        """Get if item is in Clube Leve Mais"""
        if item.find('span', {'class': 'selo_clube'}):
            return True
        return False

    def _get_item_info(self, item, deep_scrape):
        data = dict()
        if deep_scrape:
            detail_page = self._get_page(item.find('meta')['content'])
            data['ref'] = self._get_ref(detail_page)
            data['brand'] = self._get_brand(detail_page)
        data['sku'] = self._get_sku(item)
        data['name'] = self._get_name(item)
        data['clube'] = self._get_clube(item)
        data['discount'] = self._get_discount(item)
        data['price'] = self._get_price(item)
        self._store_item(**data)

    def _store_item(self, **data):
        """Saves scraped item to self.data"""
        ref = data['ref']
        del data['ref']
        self.data[ref] = data

    def scrape(self, deep_scrape=False):
        """Scrape pages in self.urls"""
        start = time.time()
        threads = list()

        for url in self.urls:
            print(url)
            num_pages = self._get_num_pages(url.format(1))
            print_pbar(0, num_pages)
            for page_num in range(1, 1 + num_pages):
                soup = self._get_page(url.format(page_num))
                items = soup.find_all('div', class_='item-product')
                for item in items:
                    t = threading.Thread(
                        target=self._get_item_info, args=(item, deep_scrape))
                    t.daemon = True
                    threads.append(t)
                    t.start()
                print_pbar(page_num, num_pages)

        for t in threads:
            t.join()

        print((time.time() - start) / 60, 'minutos')

    def export_to_dataframe(self):
        """Returns scraped data save in self.data as a pandas DataFrame"""
        df = pd.DataFrame(self.data).T
        df.index.name = 'ref'
        return df


if __name__ == "__main__":
    scraper = PagueMenosScraper(URLS)
    scraper.scrape(deep_scrape=True)
    df = scraper.export_to_dataframe()
    # df.to_csv('../output/paguemenos.csv')
