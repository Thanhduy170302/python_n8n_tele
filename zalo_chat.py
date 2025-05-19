from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time

# Zalo Web URL
ZALO_WEB_URL = "https://chat.zalo.me/"

# Zalo credentials
ZALO_PHONE_NUMBER = "84918371777"
ZALO_PASSWORD = "T@nkhanh6264@#$"

# Group name and message
GROUP_NAME = "Tổ cước P.ĐHNV"
MESSAGE = "Python developer here"

def send_message_to_zalo_group(group_name, message):
    # Initialize the Chrome driver
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))

    try:
        # Open Zalo Web
        driver.get(ZALO_WEB_URL)
        wait = WebDriverWait(driver, 20)  # Wait up to 20 seconds for elements to appear

        # Log in to Zalo Web
        phone_input = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="input-phone"]')))
        phone_input.send_keys(ZALO_PHONE_NUMBER)
        phone_input.send_keys(Keys.RETURN)

        password_input = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="input-password"]')))
        password_input.send_keys(ZALO_PASSWORD)
        password_input.send_keys(Keys.RETURN)

        # Search for the group
        search_input = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="input-search"]')))
        search_input.send_keys(group_name)
        time.sleep(2)

        # Click on the group
        group = wait.until(EC.presence_of_element_located((By.XPATH, f'//span[text()="{group_name}"]')))
        group.click()
        time.sleep(2)

        # Send the message
        message_input = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="input-chat"]')))
        message_input.send_keys(message)
        message_input.send_keys(Keys.RETURN)
        time.sleep(2)

        print("Message sent successfully!")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Close the browser
        driver.quit()

if __name__ == "__main__":
    send_message_to_zalo_group(GROUP_NAME, MESSAGE)