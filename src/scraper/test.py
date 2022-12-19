import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)



option = webdriver.FirefoxOptions()
option.set_capability("pageLoadStrategy", "eager")
driver = webdriver.Firefox(options=option)

categories = [
    'https://www.lazada.vn/trang-phuc-nu/?spm=a2o4n.home.cate_8.1.19053bdc0ehtvZ'
]

for category in categories:
    driver.get(category)
    WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.CLASS_NAME, "Bm3ON")))
    soup = BeautifulSoup(driver.page_source, features="lxml")
    products = soup.find_all(class_='Bm3ON')
    logger.info('Number of products: ' + str(len(products)))
    for product in products:
        logger.info(product.find('a')['href'][2:])

time.sleep(10)
driver.quit()
