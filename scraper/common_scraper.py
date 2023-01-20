import logging
import os
from abc import abstractmethod, ABC

import undetected_chromedriver as uc

logger = logging.getLogger(__name__)


class CommonScraper(ABC):

    def __init__(self, num_page_to_scrape, data_dir, wait_timeout, retry_num, restart_num, is_headless):
        self.restart_num = restart_num
        self.num_page_to_scrape = num_page_to_scrape
        self.data_dir = data_dir
        self.wait_timeout = wait_timeout
        self.retry_num = retry_num
        self.driver = self.start_driver(is_headless)

    def start_driver(self, is_headless):
        options = uc.ChromeOptions()
        options.set_capability("pageLoadStrategy", "none")
        if is_headless:
            options.headless = True
        
        if not os.path.exists("./profile"):
            os.mkdir("./profile")
        
        driver = uc.Chrome(options=options, user_data_dir="./profile")
        driver.set_script_timeout(10)
        driver.set_page_load_timeout(20)
        driver.maximize_window()
        return driver

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

    def restart_driver(self):
        self.driver.quit()
        self.driver = self.start_driver()
        logger.info('Driver restarted')

    # def __del__(self):
    #     self.driver.quit()
