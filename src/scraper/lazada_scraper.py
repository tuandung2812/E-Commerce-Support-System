import logging
import os

from bs4 import BeautifulSoup
from selenium.common import TimeoutException, NoSuchElementException, ElementClickInterceptedException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from .common_scraper import CommonScraper

categories = {
    'Quần áo nữ' : 'https://www.lazada.vn/trang-phuc-nu/?spm=a2o4n.home.cate_8.1.19053bdc0ehtvZ'
}

logger = logging.getLogger(__name__)


class LazadaScraper(CommonScraper):
    def __init__(self, num_page: int = 10, data_dir: str = '../data/lazada', wait_timeout: int = 5, retry_num: int = 3):
        super().__init__(num_page, data_dir, wait_timeout, retry_num)

    def get_product_urls(self):
        for category, category_url in categories.items():
            output_dir = os.path.join(self.data_dir, category)
            if not os.path.exists(output_dir):
                os.mkdir(output_dir)
            self.driver.get(category_url)

            # scrape products link
            counter = 0
            while True:
                # wait for products to be available, if not then check for popup
                for i in range(self.retry_num):
                    try:
                        WebDriverWait(self.driver, self.wait_timeout).until(
                            EC.visibility_of_element_located((By.CLASS_NAME, "Bm3ON")))
                        break
                    except TimeoutException:
                        self.check_popup()
                        self.driver.refresh()

                counter += 1
                logger.info('Scrape counter: ' + str(counter))
                curr_page_num = self.driver.find_element(By.CLASS_NAME, 'ant-pagination-item-active').get_attribute('title')
                logger.info('Current page ' + str(curr_page_num))
                soup = BeautifulSoup(self.driver.page_source, features="lxml")
                products = soup.find_all(class_='Bm3ON')
                logger.info('Number of products in page: ' + str(len(products)))
                for product in products:
                    url = product.find('a')['href'][2:]
                    self.write_to_file(url, os.path.join(category, 'url.txt'))
                logger.info("Finished scraping urls from page " + str(counter))

                # WebDriverWait(self.driver, self.wait_timeout).until(
                #     EC.visibility_of_element_located((By.CSS_SELECTOR, ".ant-pagination-next > button:nth-child(1)")))
                next_page_button = self.driver.find_element(by=By.CSS_SELECTOR, value=".ant-pagination-next > "
                                                                                      "button:nth-child(1)")
                is_last_page = not next_page_button.is_enabled()
                if counter == self.num_page or is_last_page:
                    break
                try:
                    next_page_button.click()
                    logger.info("Clicked next page")
                except ElementClickInterceptedException:
                    self.check_popup()
    def get_product_info(self):
        pass

    def check_popup(self):
        try:
            logging.info("Checking for popup")
            popup = self.driver.find_element(By.CLASS_NAME, 'baxia-dialog-close')
            logging.info("Lazada popup detected")
            popup.click()
            logging.info("Lazada popup clicked")
            return True
        except NoSuchElementException:
            logging.info("Popup not found")
            return False

    # def __del__(self):
    #     self.driver.quit()
