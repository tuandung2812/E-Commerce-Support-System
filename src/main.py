import logging
from scraper.lazada_scraper import LazadaScraper

logger = logging.getLogger('scraper')
logger.setLevel(logging.INFO)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

ch.setFormatter(formatter)

logger.addHandler(ch)

if __name__ == '__main__':
    scraper = LazadaScraper(num_page=10)
    # scraper.get_product_urls()
    scraper.get_product_info()