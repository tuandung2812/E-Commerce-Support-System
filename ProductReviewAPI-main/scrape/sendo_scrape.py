import json
import logging
from shutil import ExecError
import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

STAR_RATING_CONVERT = {
    "d7e-4e4dcb d7e-271a22" : 5.0,
    "d7e-4e4dcb d7e-cd8bf7" : 4.0,
    "d7e-4e4dcb d7e-f924b9" : 3.0,
    "d7e-4e4dcb d7e-9ac674" : 2.0,
    "d7e-4e4dcb d7e-fede87" : 1.0
    }
CHROME_DRIVER_PATH = "/home/viet/OneDrive/Studying_Materials/Introduction_to_Data_Science/EDA Project/chromedriver_linux64/chromedriver"
REVIEW_COUNTER = 0


def check_reply(review):
    """
    Check if current review is actually a reply to another review. 
    Return True if it is an reply, false otherwise.
    """
    return len(review["class"]) != 1


def scrape_from_review_list(review_list, result, max_review_num):
    """
    Scrape from the given list of reviews and store in result
    """
    global REVIEW_COUNTER
    for cur_review in review_list:
        if check_reply(cur_review):
            continue
        logging.info("Scraping a new customer")
        cur_review_dict = {}

        # customer's name
        name = cur_review.contents[1].strong.text
        logging.info(f"Got customer's name: {name}")

        # review content
        content = cur_review.contents[1].p.text
        logging.info(f"Got review text: {content}")

        # review rating
        class_name = " ".join(cur_review.contents[1].find("div", "d7e-7f502f d7e-7dd432").div["class"])
        rating = STAR_RATING_CONVERT[class_name]
        logging.info(f"Got rating: {rating}")

        # store the features into cur_review_dict
        cur_review_dict["id"] = REVIEW_COUNTER
        cur_review_dict["name"] = name
        cur_review_dict["rating"] = rating
        cur_review_dict["content"] = content

        # add this customer's review to result
        result["reviews"].append(cur_review_dict)
        logging.info(f"Added customer {name}'s review")

        # increment REVIEW_COUNTER
        REVIEW_COUNTER += 1
        
        logging.info("Number of reviews scraped: {}".format(len(result["reviews"])))
        # stop if the number of records exceeds the maxinum number
        if len(result["reviews"]) == max_review_num:
            break


def check_alert(driver):
    """
    Check if there's any alert popping up. If yes then click "No" to make it fuck off
    """

    try:
        logging.info("Checking for alert")
        alert = driver.find_element(By.CLASS_NAME, "d7e-aa34b6.e9f-f95cf0")
        logging.info("Sendo alert detected")
        alert.click()
        logging.info("Sendo alert clicked")
    except:
        logging.info("Alert not found, continuing")


def next_review_page(driver):
    """
    Find the next page button and click it.
    Return True if executed sucessfully, False otherwise
    """
    move_page_buttons = driver.find_elements(By.CLASS_NAME, "d7e-aa34b6.d7e-1b9468.d7e-13f811.d7e-2a1884.d7e-dc4b7b.d7e-d8b43f.d7e-0f6285")

    if move_page_buttons:
        logging.info("Buttons found")
        # the second one is the next page button
        forward_button = move_page_buttons[1]

        # check if forward is clickable
        if forward_button.is_enabled():
            logging.info("Forward button is clickable")

            # srcoll so that the button is visible
            driver.execute_script("arguments[0].scrollIntoView(true);", forward_button)
            driver.execute_script("window.scrollBy(0,-150)")
            logging.info("Finished scrolling to the forward button")

            check_alert(driver)
            forward_button.click()
            logging.info("Next review page clicked")
            return True
        
        logging.info("Button is not clickable. The last page is reached")
        return False
    
    logging.info("Buttons are not present")
    return False


def scrape_sendo_by_url(driver, url, max_review_num, review_check_num, review_wait_time):
    """
    Scrape users' reviews on Sendo website from a product url
    max_review_num: maxinmum number of reviews to scrape
    review_check_num: number of retries to scroll down and wait for the review section to load
    review_wait_time: waiting duration in each try
    """
    # get the page
    driver.get(url)
    review = None
    for check_num in range(1, review_check_num + 1):
        logging.info(f"Check if the reviews are present, try number: {check_num}")
        # check for alert
        check_alert(driver)
        
        # find the review section by scrolling down to 2000 until the review section is found
        try:
            driver.execute_script("window.scrollTo(0,2000)")
            review = WebDriverWait(driver, review_wait_time).until(
                            EC.presence_of_element_located((By.CLASS_NAME, "_39a-71cc39")))

            # escape the loop if the element is present
            logging.info("Review section found")
            break 
        
        except:
            pass

    # return None if cant find the reviews
    if not review:
        logging.info("Review is not found, scraping product name only")
        result = {
            "product_name": None,
            "avg_rating" : None,
            "source" : "sendo",
            "reviews" : []
        }

        # cook soup
        soup = BeautifulSoup(driver.page_source, features="lxml")

        # get product's name
        product_name = soup.find(class_="d7e-ed528f d7e-7dcda3 d7e-f56b44 d7e-fb1c84 undefined").text
        result["product_name"] = product_name
        logging.info(f"Got product's name: {product_name}")

    else:
        logging.info("Start scraping")

        # create a dictionary to store review information
        result = {
            "product_name": None,
            "avg_rating" : None,
            "source" : "sendo",
            "reviews" : []
        }
        
        # wait for average rating to be loaded before cooking soup
        WebDriverWait(driver, review_wait_time).until(
                            EC.presence_of_element_located((By.CLASS_NAME, "_39a-7b5c89")))

        # cook soup
        soup = BeautifulSoup(driver.page_source, features="lxml")

        # get product's name
        product_name = soup.find(class_="d7e-ed528f d7e-7dcda3 d7e-f56b44 d7e-fb1c84 undefined").text
        result["product_name"] = product_name
        logging.info(f"Got product's name: {product_name}")

        # average rating
        avg = soup.find(class_="_39a-7b5c89").text
        if avg:
            avg = float(avg.split("/")[0])
        result["avg_rating"] = avg
        logging.info(f"Got average rating: {avg}")

        # get the reviews
        reviews = soup.find_all(class_="_39a-71cc39")
        scrape_from_review_list(reviews, result, max_review_num)
        # stop if the number of records exceeds the maxinum number
        if len(result["reviews"]) == max_review_num:
            logging.info("Maximum number of reviews reached, stopping scraping")
            logging.info("Finished scraping")
            return result

        # grab the reviews from the other pages
        while next_review_page(driver):
            logging.info("Scraping the new page")
            reviews = soup.find_all(class_="_39a-71cc39")
            scrape_from_review_list(reviews, result, max_review_num)

            # stop if the number of records exceeds the maxinum number
            if len(result["reviews"]) == max_review_num:
                logging.info(f"Maximum number of reviews reached({max_review_num}), stopping scraping")
                break
            # time.sleep(1)

    logging.info("Finished scraping")
    return result


def scrape_sendo(driver, input, product_num=3, max_review_num=20, review_check_num=10, review_wait_time=0.1, search_wait_time=5, verbose=False):
    """
    Scrape users' reviews on Sendo website from either a url or a product name specified in input
    input: string containing a url or a product name
    num_product: number of product to be scraped when the input is a product name
    max_review_num: maxinmum number of reviews to scrape
    review_check_num: number of retries to scroll down and wait for the review section to load
    review_wait_time: waiting duration in each review wating attempt
    search_wait_time: waiting duration for the product in the search page to be loaded
    verbose: show running info
    """
    if verbose:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
    else:
        logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s')

    if input.__contains__("sendo.vn"):
        logging.info("Input is a url")
        return scrape_sendo_by_url(driver, input, max_review_num, review_check_num, review_wait_time)
    else:
        logging.info("Input is a product name")
        driver.get("https://www.sendo.vn/tim-kiem?q={}".format(input))
        try:
            WebDriverWait(driver, search_wait_time).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, "a[href^=\"https://www.sendo.vn\"][target=_blank]")))
            logging.info("Search page finished loading")
        except Exception as e:
            logging.error(e)

        # cook soup
        soup = BeautifulSoup(driver.page_source, features="lxml")

        # get product
        product_list = soup.find_all(class_="d7e-f7453d d7e-57f266")
        
        result_list = []
        for i, product in enumerate(product_list):
            if i == product_num:
                break
            product_url = product.contents[0]["href"]
            result_list.append(scrape_sendo_by_url(driver, product_url, max_review_num, review_check_num, review_wait_time))
        return result_list


if __name__ == "__main__":
    url = [
        "https://www.sendo.vn/op-lung-iphone-6-plus-24661530.html?source_block_id=feed&source_page_id=home&source_info=desktop2_60_1653209392128_34e4536f-bc6a-4a91-9335-ace33da5bcc1_-1_ishyperhome0_0_57_22_-1", # 1 page
        "https://www.sendo.vn/combo-6-goi-khan-uot-khong-mui-unifresh-vitamin-e-khan-uot-tre-em-80-mienggoi-23796514.html?source_block_id=feed&source_page_id=home&source_info=desktop2_60_1653306028025_17f349b3-3df6-4ea5-9ae7-ae076bbd7e9b_-1_ishyperhome0_0_2_1_-1", # many pages
        "https://www.sendo.vn/op-lung-iphone-6-plus-6s-plus-chong-soc-360-17878754.html?source_block_id=feed&source_page_id=home&source_info=desktop2_60_1653314071212_17f349b3-3df6-4ea5-9ae7-ae076bbd7e9b_-1_ishyperhome0_0_11_22_-1", # 2 pages
        "https://www.sendo.vn/tu-lanh-lg-inverter-door-in-door-601-lit-gr-x247js-24825976.html?source_block_id=feed&source_page_id=search&source_info=desktop2_60_1655489127639_undefined_-1_cateLvl2_0_2_23_-1" # no review
    ]
    product_name = "tủ lạnh lg"
    chrome_options = Options()
    # chrome_options.add_argument("--headless")

    # start a webdriver
    start = time.time()
    driver = webdriver.Chrome(CHROME_DRIVER_PATH, options=chrome_options)
    result = scrape_sendo(driver, product_name, max_review_num=2, verbose=True)
    driver.quit()
    print("Time taken: ", time.time() - start)
    pretty = json.dumps(result, indent=4, ensure_ascii=False)
    print(pretty)
    