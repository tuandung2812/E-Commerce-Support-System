from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import time
from selenium.webdriver.common.action_chains import ActionChains
import json
import bs4, requests

def scrape_lazada(driver, url, max_comment = 5):

    driver.get(url)
    result = {'product_name':'','avg_rating':0,'source':'lazada','reviews':[]}
    review_count = 0
    result['product_name'] = driver.find_element(By.CSS_SELECTOR, "[class='pdp-mod-product-badge-title']").text


    #click out pop up
    ac = ActionChains(driver)
    ac.move_by_offset(1, 1).click().perform()

    x = 0
    while True:
        y = 1000
        for timer in range(0,8):
             driver.execute_script("window.scrollTo(0, "+str(y*0.4)+")")
             y += 1000
             time.sleep(1)

        # driver.implicitly_wait(5)
        # driver.execute_script("window.scrollTo(0, document.body.scrollHeight*0.8);")
        try:
            product_reviews = WebDriverWait(driver,2).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR,"[class='item']")))
        except:
            break
        result['avg_rating'] = float(driver.find_element(By.CSS_SELECTOR, "[class='score-average']").text)

        # Get product review
        for product in product_reviews:
            if x < max_comment:
                review = {}
                details = product.find_element(By.CSS_SELECTOR, "[class='middle']")
                details= details.find_elements(By.TAG_NAME,'span')
                review['id']=review_count
                review_count +=1
                review['name'] = details[0].text[3:]
                # review['status'] = details[1].text
                # review['date'] = product.find_element(By.CSS_SELECTOR, "[class='title right']").text
                review['rating'] = len(product.find_elements(By.CSS_SELECTOR, "[src='//laz-img-cdn.alicdn.com/tfs/TB19ZvEgfDH8KJjy1XcXXcpdXXa-64-64.png']"))
                review['content'] = product.find_element(By.CSS_SELECTOR, "[class='content']").text
                if review != "" or review.strip():
                    # print(review, "\n")
                    result['reviews'].append(review)
                    x += 1
            else:
                break

        if x < max_comment:
            #Check for next button to click. If no button found, exit loop.
            if len(driver.find_elements(By.CSS_SELECTOR,"button.next-pagination-item.next[disabled]")) == 0:
                break
            else:
                try:
                    button_next = WebDriverWait(driver, 2).until(
                        EC.visibility_of_element_located((By.CSS_SELECTOR, "button.next-pagination-item.next")))
                    driver.execute_script("arguments[0].click();", button_next)
                    continue
                except: break
        else:
            break

    return result


def scrape_lazada_by_product(driver, input, max_page = 5, max_comment_per_page = 5):
    result = {'query': input, "result":[]}
    query_url = 'https://www.google.com/search'

    request_result= requests.get(query_url, params={'q': f'site:lazada.vn/products {input}'})

    soup = bs4.BeautifulSoup(request_result.text, "html.parser")
    prod_links = []
    for a in soup.find_all('a', href=True):
        if '/url?q=https://www.lazada.vn/products/' in a['href']:
            prod_links.append(a['href'][7:])
    limit = min(max_page, len(prod_links))
    for i in range(limit):
        result["result"].append(scrape_lazada(driver, prod_links[i], max_comment_per_page))
    return result

if __name__ == '__main__':
    try:
        driver = webdriver.Chrome("D:/chromedriver.exe")
    except:
        from webdriver_manager.chrome import ChromeDriverManager
        driver = webdriver.Chrome(ChromeDriverManager().install())

    # url = r'https://www.lazada.vn/products/dien-thoai-apple-iphone-13-pro-max-128gb-i1522497182-s6393590575.html?search=1&spm=a2o4n.searchlistcategory.list.i72.75bf3a1fbXB2jM'
    # test = scrape_lazada(driver, url, 4)

    test = scrape_lazada_by_product(driver, "tủ lạnh LG")

    driver.close()
    test_result = json.dumps(test, indent=4, ensure_ascii=False)
    print(test_result)
