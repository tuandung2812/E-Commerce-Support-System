import json
import logging
import os
import time

from bs4 import BeautifulSoup
from selenium.common import TimeoutException, ElementClickInterceptedException, \
    MoveTargetOutOfBoundsException, StaleElementReferenceException
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.ui import WebDriverWait

from src.scraper.common_scraper import CommonScraper

categories = {
    'Áo Khoác': 'https://shopee.vn/%C3%81o-Kho%C3%A1c-cat.11035567.11035568'
}

logger = logging.getLogger(__name__)


class ShopeeScraper(CommonScraper):
    def __init__(self, num_page_to_scrape=10, data_dir='../data/shopee', wait_timeout=5, retry_num=3,
                 restart_num=10):
        if not os.path.exists(data_dir):
            os.mkdir(data_dir)
        super().__init__(num_page_to_scrape, data_dir, wait_timeout, retry_num, restart_num)

    def get_product_urls(self):
        for category, category_url in categories.items():
            logger.info("Scraping category: " + category)
            output_dir = os.path.join(self.data_dir, category)
            if not os.path.exists(output_dir):
                os.mkdir(output_dir)
            self.driver.get(category_url)

            # iterate through each page and scrape products link
            counter = 1
            prev_url = ''
            while True:
                logger.info("Scraping urls from page " + str(counter))
                if prev_url == self.driver.current_url:
                    logger.info('This page is identical to the previous page, which means last page is reached')
                    break
                prev_url = self.driver.current_url
                for retry in range(self.retry_num):
                    try:
                        self.driver.execute_script("window.scrollTo(0,0)")
                        WebDriverWait(self.driver, self.wait_timeout).until(
                            ec.visibility_of_element_located((By.CLASS_NAME, "shopee-search-item-result__item")))
                        for i in range(12):
                            self.driver.execute_script("window.scrollBy(0,350)")
                            time.sleep(0.05)

                        soup = BeautifulSoup(self.driver.page_source, features="lxml")
                        product_list = soup.find_all(class_='shopee-search-item-result__item')
                        product_list_with_url = soup \
                            .find(class_='row shopee-search-item-result__items') \
                            .find_all('a', href=True)
                        num_product = len(product_list)
                        num_product_with_url = len(product_list_with_url)
                        logger.info(f'Found {num_product} products')
                        logger.info(f'Found {num_product_with_url} products with url')

                        if len(product_list_with_url) == len(product_list):
                            logger.info('All 60 products are loaded, extracting product url')
                            self._get_product_urls(product_list, category)
                            break
                        elif retry == self.retry_num - 1:
                            logger.info(f'Only {num_product_with_url}/{num_product} product url are found '
                                        f'after retrying {self.retry_num} times')
                        else:
                            logger.info(f'{num_product_with_url}/{num_product} product url are found, retrying')

                    except TimeoutException:
                        logger.info(f'Products not visible after waiting for {self.wait_timeout}, retrying')

                counter += 1
                if counter == self.num_page_to_scrape:
                    logger.info(f'Reached maximum number of pages to scrape in category: {category}')
                    break

                logger.info('Going to the next page')
                next_page_button = self.driver.find_element(by=By.CLASS_NAME, value="shopee-icon-button--right")
                next_page_button.click()

    def _get_product_urls(self, product_list, category):
        for product in product_list:
            product_url = 'https://shopee.vn' + product.a['href']
            self.write_to_file(product_url, os.path.join(category, 'url.txt'))

    def get_product_info(self):
        pass
