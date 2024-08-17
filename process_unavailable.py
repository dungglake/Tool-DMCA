import pickle
import os
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium import webdriver

cookies_file_path = "C:\\Users\\Admin\\Downloads\\selenium_test\\cookies.pkl"

def process_unavailable_urls(url):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-infobars")
    chrome_options.add_argument("--window-size=1920,1080")  
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    driver = webdriver.Chrome(options=chrome_options)
    try:
        driver.get("https://www.dmca.com/add/upload-content.aspx#popup-pages") 
        with open(cookies_file_path, "rb") as f:
                cookies = pickle.load(f)
                for cookie in cookies:
                    driver.add_cookie(cookie)
        driver.refresh()
        
        WebDriverWait(driver, 20).until(lambda driver: driver.execute_script("return document.readyState") == "complete")
        
        WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.ID, "pages-paste-link")))
        driver.find_element(By.ID, "pages-paste-link").send_keys(url)
        
        actions = ActionChains(driver)
        actions.send_keys(Keys.TAB).perform()   

        page_title_input = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.ID, "page-title")) 
        )

        original_value = page_title_input.get_attribute("value")

        WebDriverWait(driver, 20).until(lambda driver: page_title_input.get_attribute("value") != original_value)

        button_element = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, 'button.addProtectionPages'))
        )
        
        driver.execute_script("arguments[0].scrollIntoView(false);", button_element)
        
        actions = ActionChains(driver)
        actions.move_to_element(button_element).click().perform()
                    
        WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.XPATH, '//div[@class="block-confirmation step-one step-two"]')))
    except Exception as e:
        print(f"Error processing URL {url}: {e}")
        return "Failed to submit"