import logging
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium import webdriver

def check_and_update_dmca_status(url):
    logging.info(f"Đang kiểm tra trạng thái DMCA cho URL: {url}")
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-infobars")
    chrome_options.add_argument("--window-size=1920,1080")  
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    driver = webdriver.Chrome(options=chrome_options)
    try:
        driver.get(f'https://www.dmca.com/Protection/Status.aspx?ID=ce976549-01f7-44cf-803b-8a883b40460d&refurl={url}')

        status_element = WebDriverWait(driver, 20).until(
            EC.visibility_of_element_located((By.XPATH, '//div[@class="page-status"]//p[@class="tooltip"]/span[contains(@class, "asset-status")]'))
        )
            
        status_class = status_element.get_attribute("class")

        if "green-text" in status_class:
            status_text = "Active"
        elif "checking-text" in status_class:
            status_text = "Pending"
        elif "red-text" in status_class:
            status_text = "Unavailable"

    except Exception as e:
        logging.error(f"Lỗi khi kiểm tra URL '{url}': {e}")
    finally:
        driver.quit()

    return status_text
