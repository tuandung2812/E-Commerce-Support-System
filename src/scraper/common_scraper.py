import logging
import os
from abc import abstractmethod, ABC

from selenium import webdriver

logger = logging.getLogger(__name__)


class CommonScraper(ABC):

    def __init__(self, num_page, data_dir, wait_timeout, retry_num):
        self.num_page = num_page
        self.data_dir = data_dir
        self.wait_timeout = wait_timeout
        self.retry_num = retry_num

        option = webdriver.FirefoxOptions()
        option.set_capability("pageLoadStrategy", "eager")
        driver = webdriver.Firefox(options=option)
        self.driver = driver

    def write_to_file(self, text, file_name):
        full_path = os.path.join(self.data_dir, file_name)
        with open(full_path, 'a') as f:
            f.write(text + '\n')

    @abstractmethod
    def get_product_urls(self):
        pass

    @abstractmethod
    def get_product_info(self):
        pass
