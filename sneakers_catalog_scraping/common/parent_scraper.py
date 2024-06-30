import json
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from common.brand_scraper import BrandScraper

class ParentScraper():

    def __init__(self):
        driver = webdriver.Chrome()
        self.driver = driver
        self.parent_url = 'https://sneakers123.com/en/models'
        self.wait = WebDriverWait(driver, 10)
        self.action = ActionChains(driver)

    def get_driver(self):
        return self.driver

    def get_parent_url(self):
        return self.parent_url

    # General Scraping Static Functions
    @staticmethod
    def get_total_from_elem(element, class_name):
        try:
            total = element.find_element(By.CLASS_NAME, class_name).text
            total = total if total != '' else 0
        except Exception as e:
            print(e.__class__.__name__)
            total = 0
        return int(total)


    # Scraping run function
    def run(self):
        with self.driver as driver:
            driver.get(self.parent_url)
            brands = BrandScraper(self.driver, self.parent_url, self.wait, self.action).process_brands()
            # Write brands dictionary as json file locally.
            with open('brands.json', 'w', encoding='utf-8') as file:
                file.write(json.dumps(brands))