import pickle
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

cookies_file_path = "C:\\Users\\Admin\\Downloads\\selenium_test\\cookies.pkl"

def login_and_save_cookies(driver):
    driver.get("https://www.dmca.com/add/login.aspx?r=myasset-notloggedin")
    driver.find_element(By.ID, "username").send_keys("vietnix.net@gmail.com")  
    driver.find_element(By.ID, "password").send_keys("1HQ4VzdD")  
    driver.find_element(By.CSS_SELECTOR, ".btn--block.button__login").click()
    
    WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.XPATH, '//div[@class= "search asset-search"]')))

    cookies = driver.get_cookies()
    with open(cookies_file_path, "wb") as f:
        pickle.dump(cookies, f)