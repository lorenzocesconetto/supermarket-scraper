import unittest
from scripts.pague_menos import PagueMenosScraper
from bs4 import BeautifulSoup as bs4


class TestPagueMenosScraper(unittest.TestCase):
    def setUp(self) -> None:
        self.scraper = PagueMenosScraper([''])
        with open('tests/index.html', 'r') as f:
            document = f.read()
        self.soup = bs4(document, 'html.parser')
        items = self.soup.find_all('div', class_='item-product')
        self.item_1 = items[0]
        self.item_2 = items[0]

    def test_get_name(self):
        self.assertEqual(self.scraper._get_name(self.item_1),
                         'chicken perdigao queijo 1 unidade 250g')


if __name__ == "__main__":
    unittest.main()
