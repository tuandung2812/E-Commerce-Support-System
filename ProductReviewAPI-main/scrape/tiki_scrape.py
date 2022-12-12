from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from datetime import datetime, timedelta
import logging


CHROME_DRIVER_PATH = "D:/chromedriver.exe"
TODAY = datetime.today()


def scrape_tiki_by_url(driver, url, max_review_num=10, review_wait_time=10):
    """
    Function to scrape reviews on tiki by url

    args:
    max_review_num: Maximum number of reviews to scrape
    review_wait_time: Maximum time (in seconds) to wait for review to appear
    """
    results = []

    # get the page (there's a bug with webdriver 103 -> try getting the page until success)
    while True:
        try:
            driver.get(url)
            driver.execute_script("window.scrollTo(0,4000)")
        except WebDriverException:
            continue
        finally:
            break
    try:
        review = WebDriverWait(driver, review_wait_time).until(
            EC.presence_of_element_located((By.CLASS_NAME, "review-comment__rating-title")))
        assert review is not None
    except Exception as e:
        logging.info("No review-comment__rating-title")

    # cook soup
    soup = BeautifulSoup(driver.page_source, features="lxml")
    reviews = soup.find_all("div", class_="review-comment__content")

    try:
        product_name = soup.find_all("h1", class_="title")[0].text
    except:
        product_name = url
        print(url)

    if reviews:
        review_id = 0
        next_page_button = next_review_page(driver)
        while next_page_button and len(results) < max_review_num:
            soup = BeautifulSoup(driver.page_source, features="lxml")
            texts = soup.find_all(class_="review-comment__content")
            ratings = soup.find_all('div', class_="Stars__StyledStars-sc-1g6jwue-0 bkVtVK review-comment__rating")
            buyer_names = soup.find_all(class_="review-comment__user-name")

            assert len(texts) == len(ratings)

            for text, rating, buyer_name in zip(texts, ratings, buyer_names):
                review_dict = {
                    "id": review_id,
                    "name": buyer_name.text.strip(),
                    "rating": float(len(rating.find_all(stroke="#FFA142", fill='#FFD52E'))-5),
                    "content": text.text
                }
                review_id += 1
                results.append(review_dict)

            try:
                next_page_button.click()
                next_page_button = next_review_page(driver)
            except Exception as e:
                # No more review page left
                break

    try:
        avg_rating = soup.find_all("div", class_="review-rating__point")[0]
        avg_rating = float(avg_rating.text)
        results = remove_dup(results)
        results = {
            "product_name": product_name,
            "source": "tiki",
            "avg_rating": avg_rating,
            "reviews": results
        }
    except IndexError:
        # Product has no review
        results = {
            "product_name": product_name,
            "source": "tiki",
            "avg_rating": None,
            "reviews": []
        }
    print(results)
    return results


def next_review_page(driver):
    try:
        move_page_button = driver.find_element(By.CLASS_NAME, "btn.next")
        driver.execute_script("arguments[0].scrollIntoView(true);", move_page_button)
        return move_page_button
    except:
        return None


def remove_dup(L):
    res = []
    for i in L:
        if i not in res:
            res.append(i)
    return res


def scrape_tiki_by_name(driver, input, product_num=3, max_review_num=20, review_wait_time=5):
    """
    Scrape reviews from tiki by product name

    args:
    input: Product name
    product_num: Number of products to scrape reviews
    max_review_num: Maximum number of reviews to scrape for each product
    review_wait_time: Maximum time (in seconds) to wait for review to appear
    """
    result_list = []
    page_num = 1
    while len(result_list) != product_num:
        if page_num == 1:
            page_url = f"https://tiki.vn/search?q={'+'.join(input.split())}"
        else:
            page_url = f"https://tiki.vn/search?q={'+'.join(input.split())}&page={page_num}"
        while True:
            try:
                driver.get(page_url)
                driver.execute_script("window.scrollTo(0,6000)")
            except WebDriverException:
                continue
            finally:
                break

        # cook soup
        soup = BeautifulSoup(driver.page_source, features="lxml")

        # get product
        product_list = soup.find_all(class_="product-item")

        for i, product in enumerate(product_list):
            if i == product_num:
                break
            if "tka" in product["href"]:
                product_url = "https:" + product["href"]
            else:
                product_url = "https://tiki.vn" + product["href"]

            product_review = scrape_tiki_by_url(driver, product_url, max_review_num, review_wait_time)
            result_list.append(
                product_review
            )

        # increase page number by 1
        page_num += 1
    return result_list


def scrape_tiki(driver, input, product_num=3, max_review_num=20, review_wait_time=5):
    """
    Function to scrape reviews from tiki, whether by name or by url

    args:
    input: url or product name
    product_num: Maximum number of product to scrape if scrape by name
    max_review_num: Maximum number of reviews to scrape for each product
    review_wait_time: Maximum time (in seconds) to wait for review to appear
    """
    if "tiki.vn" in input:
        return scrape_tiki_by_url(driver, input, max_review_num=max_review_num, review_wait_time=review_wait_time)
    else:
        return scrape_tiki_by_name(driver, input, product_num=product_num, max_review_num=max_review_num, review_wait_time=review_wait_time)


if __name__ == "__main__":
    url = "https://tiki.vn/tai-nghe-bluetooth-bt-01-khong-day-kieu-dang-the-thao-mau-ngau-nhien-p4810215.html?spid=48872460"
    # url = "https://tiki.vn/may-xay-sinh-to-va-lam-sua-hat-da-nang-tefal-bl967b66-1300w-luoi-dao-voi-cong-nghe-powelix-hang-chinh-hang-p79561024.html?spid=79561025"
    # url = "https://tiki.vn/o-dien-da-nang-chong-giat-3-cong-usb-va-9-o-cam-icart-p35876754.html?spid=59452050"
    # url = r"https://tka.tiki.vn/pixel/pixel?data=djAwMSRsQZlaHR_2h71ndhYc0OsR6ajNcZjMuOaJt5IkoDyg6e9-0uL2yGq_X9BaHy8SUfI1O6XccbVLaMtKhV6YlL9AcwVgqI5H23i2zfnUwE_mhHFz9LlTvjRw7fe3dVM0uaKQVi_74aoBTSGEEgwwRhvgxGVxQpqKKyaVzZlV-393vjt2-RKSVbHuwrxDyjZ4jfDODSRyV5cy2DER66HBbiRwUXltSraNMCu0MEE_p2PKpI7UaFge2IVHpGhHugpbtDKj2JcCNE18sgWJLcMJaIO00TS14QDKNNBsgyT_Dc1-gejAOa3PwjWI3z-s9u8le5n4UsAzqvrbWVJ-XLLe9JyW1-qsJG8CBCHe9Y5gAQgbsY73xWEYpUOaKO7nZtsy69Rkdu8-2AUGl38xUWSGRfz-PCNrW_A_tPA_UO3n6YUE0CcAMX5HtyJ-f0wJ2Wkm9cytz_yGs7iLEB1G4092qPyj7-SeEAFeMFOVTSOkd9G6f-omXD7qNfsDPvVnAOV048hKkN-a-592CGcXamKWHIbq1ZAdEczUZRgC0rK47toI9h8_iLXrDf_0OKdFsqMHwzQuMpRxexU_NljSCBK87Lzh5GKuGHiImKMOyit0wOF8C5blrzOAS9P5cFG8qpV0E9Xjl18UB4daEMg3wlPg2xidCSN4sRLgvsPChHt6WIdWdnOFwU2jIOYUEkMmsy8bIahkS64QXH4ErJf7OzamsIJnBTMYTRSB1Z5OcmEcu0O837LSYcey8G1Zo8qhpF2g-r6UTCK6LudQaszrUA6w1VftnEF0N3IhkO8GXGGUgS23pZj25qJIqu_d5v0I3ubB5l2iv7VRohFxCO-lSoAujwuPSSkf2QtwUm9_Xfn4P-Ongvd9SJfkpId1yfbymMqLjIXViXoi8EsIiGYKb8N8-u2KapZgHdr1oLvDTBYGFVVO8f9G5ECgFU4B5d8Y-VTW3VyCNAtZDEwX1HqTsl-Witv50E3rD6bJvCInOEq7sPCVoR_6_TAIHE159V2d0RmD_wmgspqZYF_2gnE1LoBljMvz8iiuvWw1NRxM8FhFilgKnX4BVZoXa_yolt0BHf8y99dGt4CXOWR_vMHfoHQCsAQRpZOiECQmmjX3nauCv4P7xsjnsfcb1_v0VmQhoUiV9S-NvWzMU1Wtr0j2Z0Fqv8W7xLnhkH2gzWHPtAAnZz16OImg8OWVQeBDiZiRqur72xCVxLObXZMjt9FmJYpEqEH9LE_yFgxR7ZAhzMFvRK2IrGpB6UWfiOO6Rc-wzSDVP1w-VT351h7yib96H2btNOkqss6YjbRflrIpJTdAq2vgHQ&CLICK&reqid=TE10dyKkZi&pos=1&redirect=https%3A%2F%2Ftiki.vn%2Ftai-nghe-khong-day-chup-lai-remax-rb-620hb-hang-chinh-hang-p113569143.html%3Fspid%3D113569145"
    # product = "tai nghe"

    chrome_options = Options()
    driver = webdriver.Chrome(CHROME_DRIVER_PATH, options=chrome_options)
    # scrape_tiki(driver, product, product_num=70)
    scrape_tiki_by_url(driver, url, max_review_num=20)

    pass