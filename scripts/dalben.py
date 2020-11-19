import pandas as pd
from typing import Dict, List

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException, TimeoutException

from utils import get_text_excluding_children, print_pbar

CHROME_DRIVER_PATH = '/Users/lorenzocesconetto/chromedriver'
CHROME_OPTIONS = Options()
CHROME_OPTIONS.add_argument("--headless")
CHROME_OPTIONS.add_argument("--disable-extensions")
CHROME_OPTIONS.add_argument("--incognito")
DRIVER = webdriver.Chrome(CHROME_DRIVER_PATH, options=CHROME_OPTIONS)

BASE_URL = 'https://www.dalbendelivery.com.br'
URLS = [
    'https://www.dalbendelivery.com.br/produtos/departamento/bebidas',
    'https://www.dalbendelivery.com.br/produtos/departamento/mercearia',
    'https://www.dalbendelivery.com.br/produtos/departamento/saudaveis',
    'https://www.dalbendelivery.com.br/produtos/departamento/hortifruti',
    'https://www.dalbendelivery.com.br/produtos/departamento/padaria-e-pizzaria',
    'https://www.dalbendelivery.com.br/produtos/departamento/acougue',
    'https://www.dalbendelivery.com.br/produtos/departamento/peixaria',
    'https://www.dalbendelivery.com.br/produtos/departamento/frios',
    'https://www.dalbendelivery.com.br/produtos/departamento/laticinios-e-conservas',
    'https://www.dalbendelivery.com.br/produtos/departamento/congelados',
    'https://www.dalbendelivery.com.br/produtos/departamento/limpezas',
    'https://www.dalbendelivery.com.br/produtos/departamento/bazar',
    'https://www.dalbendelivery.com.br/produtos/departamento/pet-e-jardim',
    'https://www.dalbendelivery.com.br/produtos/departamento/beleza-e-cuidados',
]


class DalbenScraper:
    def __init__(self, driver: webdriver.Chrome, urls: List[str]) -> None:
        self.driver = driver
        self.data = dict()
        self.urls = urls
        self.urls_to_scrape = None
        self.wait_time = 5

    def _get_urls_to_scrape(self) -> None:
        urls_to_scrape = list()
        for url in self.urls:
            self.driver.get(url)
            try:
                elements = WebDriverWait(self.driver, self.wait_time).until(
                    EC.presence_of_all_elements_located(
                        (
                            By.CSS_SELECTOR,
                            'div.row.vip-categories p > a[href^="/produtos/departamento/"]'
                        )
                    )
                )
                results = [element.get_attribute('href')
                           for element in elements]
                urls_to_scrape = urls_to_scrape + results
            except:
                continue
        self.urls_to_scrape = urls_to_scrape

    def _get_num_pages(self) -> int:
        try:
            pag = WebDriverWait(self.driver, self.wait_time).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, 'app-paginacao > nav > div'))
            )
            num = pag.text.split()[3]
            num = num.replace(',', '')
            return int(num)
        except (NoSuchElementException, TimeoutException) as e:
            return 1

    def _get_url_info(self, url: str) -> Dict[str, str]:
        meta = url.split('/')
        categoria = meta[-2]
        subcategoria = meta[-1]
        return {'categoria': categoria, 'subcategoria': subcategoria}

    def _get_ref(self, sub_item) -> int:
        return int(sub_item.get_attribute('href').split('/')[5])

    def _get_price(self, item) -> float:
        try:
            price_tag = item.find_element_by_css_selector(
                'div.drill-price > div.info-price')
        except NoSuchElementException:
            return -1
        price = get_text_excluding_children(self.driver, price_tag)
        price = price.replace(',', '.')
        return float(price.split()[1])

    def _get_name(self, sub_item) -> str:
        return sub_item.get_attribute('title')

    def scrape(self):
        if self.urls_to_scrape is None:
            self._get_urls_to_scrape()

        for i, url in enumerate(self.urls_to_scrape):
            print(i, '/', len(self.urls_to_scrape), url)
            self.driver.get(url)
            num_pages = self._get_num_pages()
            print_pbar(0, num_pages)
            url_info = self._get_url_info(url)
            for page_num in range(1, 1 + num_pages):
                url = url + '?page=' + str(page_num)
                try:
                    items = WebDriverWait(self.driver, self.wait_time).until(
                        EC.presence_of_all_elements_located(
                            (By.CSS_SELECTOR, 'div.product'))
                    )
                except:
                    print('Timeout at:', url)
                    continue

                for item in items:
                    price = self._get_price(item)
                    sub_item = item.find_element_by_css_selector(
                        'p > a[title]'
                    )
                    name = self._get_name(sub_item)
                    ref = self._get_ref(sub_item)

                    self.data[ref] = {'price': price, 'name': name, **url_info}
                print_pbar(page_num, num_pages)

    def export_to_dataframe(self) -> pd.DataFrame:
        """Returns scraped data save in self.data as a pandas DataFrame"""
        df = pd.DataFrame(self.data).T
        df.index.name = 'ref'
        return df


if __name__ == "__main__":
    scraper = DalbenScraper(DRIVER, URLS)
    scraper._get_urls_to_scrape()
    scraper.scrape()
    df = scraper.export_to_dataframe()
    df.to_csv('../output/Dalben.csv')
