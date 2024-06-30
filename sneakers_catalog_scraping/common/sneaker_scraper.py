
import json
import platform
from time import sleep
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import (
        TimeoutException,
        StaleElementReferenceException,
        NoSuchElementException
    )
import re

class SneakerScraper():

    def __init__(self, driver, parent_url, wait, action, brand, sneaker_elems, index):
        self.driver = driver
        self.parent_url = parent_url
        self.wait = wait
        self.action = action
        self.brand = brand
        self.sneaker_elems = sneaker_elems
        self.index = index

    def refresh_sneakers(self, sneakers):
        driver = self.driver
        wait = self.wait

        try:
            sneakers[0].text
        except Exception as e:
            # Print exception type
            print("Refreshing sneakers list...")
            print(e.__class__.__name__)
            driver.refresh()
            wait.until(
                EC.presence_of_element_located((By.TAG_NAME, 'section'))
            )
            sneakers = driver.find_elements(By.TAG_NAME, 'section')
            # sleep(5)
        return sneakers
    

    # Process one sneaker function
    def process_sneaker_unit(self):
        driver = self.driver
        action = self.action
        wait = self.wait
        brand = self.brand

        sneaker = {
                'brand': brand,
                'properties': None
            }

        sneakers_elems = self.sneaker_elems
        sneakers_elems = self.refresh_sneakers(sneakers_elems)
        sneaker_clickable = sneakers_elems[self.index].find_element(By.TAG_NAME, 'a')

        image_url = sneaker_clickable.find_element(By.CLASS_NAME, 'product-photo') \
            .find_element(By.TAG_NAME, 'img').get_attribute('src')


        until_in = EC.presence_of_element_located((By.TAG_NAME, 'tbody'))
        try:
            SneakerScraper.cmdclick_and_wait(
                sneaker_clickable,
                self.action,
                self.wait,
                until_in
            )
        except TimeoutException:
            # If url contains /page/number go to previous page and then go to next page again.
            print('TimeoutException')
            if '/page/' in driver.current_url:
                excpt = True
                while excpt:
                    SneakerScraper.back_and_forward(driver, action, wait)
                    sneakers_elems = self.refresh_sneakers(sneakers_elems)
                    sneaker_clickable = sneakers_elems[self.index].find_element(By.TAG_NAME, 'a')
                    try:
                        SneakerScraper.cmdclick_and_wait(sneaker_clickable, action,  wait, until_in)
                        excpt = False
                    except TimeoutException:
                        continue
            # https://sneakers123.com/en/adidas-gazelle-not-found-sku-function
            # Page Not Found
            else:
                tbody_error = 'tbody not exist'
                print(tbody_error)
                url = driver.current_url
                brand_url = url
                # if '/en/' not in brand_url:
                #     # Insert /en/ between ".com" and "/sneakers/" 
                #     brand_url = re.sub(r'(.+\.com)(/sneaker/.*)', r'\1/en\2', brand_url)
                #     print(brand_url)
                #     self.driver.get(brand_url)
                #     sleep(2)
                # driver.back()
                error_resp = {
                    "brand": brand,
                    "properties" : {
                        "error": tbody_error,
                        "url": url
                        }
                    }
                print(error_resp)
                return error_resp

        sneaker_url = driver.current_url
        tbody = driver.find_element(By.TAG_NAME, 'tbody')
        properties = SneakerScraper.extract_sneaker_properties(tbody)
        properties['sneaker_url'] = sneaker_url
        properties['sneaker_image'] = image_url
        sneaker['properties'] = properties
        print(properties)
        return sneaker

    # Static functions
    @staticmethod
    def click_and_wait(clickable, action, wait, until_in):
        action.click(clickable).perform()
        wait.until(until_in)

    @staticmethod
    def go_back_or_forward(driver, action, wait, until_in, b_or_f='›'):
        driver.refresh()
        try:
            pages_box = driver.find_element(By.CLASS_NAME, 'pagination')
            clickable_next_page = pages_box.find_element(By.LINK_TEXT, b_or_f)
            SneakerScraper.click_and_wait(clickable_next_page, action, wait, until_in)
            sleep(5)
        except StaleElementReferenceException:
            driver.refresh()
            wait.until(
                EC.presence_of_element_located((By.CLASS_NAME, 'pagination'))
            )
            pages_box = driver.find_element(By.CLASS_NAME, 'pagination')
            clickable_next_page = pages_box.find_element(By.LINK_TEXT, b_or_f)
            SneakerScraper.click_and_wait(action, clickable_next_page, wait, until_in)

    @staticmethod
    def back_and_forward(driver, action, wait):
        excpt = True
        while excpt:
            try:
                # Go prev page
                until_in = EC.presence_of_element_located((By.CLASS_NAME, 'pagination'))
                SneakerScraper.go_back_or_forward(driver, action, wait,until_in, '‹')
                # Go next page
                SneakerScraper.go_back_or_forward(driver, action, wait,until_in)
                excpt = False
            except NoSuchElementException:
                continue

        
    @staticmethod
    def extract_sneaker_properties(tbody):
        properties = {}
        for tr in tbody.find_elements(By.TAG_NAME, 'tr'):
            tds = tr.find_elements(By.TAG_NAME, 'td')
            # Transform tds[0] into snake case.
            prop = tds[0].text
            prop = prop.replace(' ', '_').lower()
            properties[prop] = tds[1].text
        return properties
    
    @staticmethod
    def cmdclick_and_wait(clickable, action, wait, until_in):
        action.key_down(Keys.COMMAND if 'darwin' in platform.system().lower() else Keys.CONTROL)\
            .click(clickable)\
            .key_up(Keys.COMMAND if 'darwin' in platform.system().lower() else Keys.CONTROL)\
            .perform()
        # Open shoe link.
        wait.until(until_in)


    

    