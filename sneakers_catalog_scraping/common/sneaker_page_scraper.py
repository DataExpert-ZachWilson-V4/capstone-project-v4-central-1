
import platform
import re
import json
from time import sleep
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import (
        NoSuchElementException,
        TimeoutException,
        StaleElementReferenceException
    )
from common.sneaker_scraper import SneakerScraper
import os

class SneakerPageScraper():


    def __init__(self, driver, parent_url, wait, action, brand_a):
        self.driver = driver
        self.parent_url = parent_url
        self.wait = wait
        self.action = action
        self.clickable_brand = brand_a
        self.brand_name = brand_a.text
        # Print brand_a url
        self.brand_url = brand_a.get_attribute('href')


    def open_new_tab(self):
        until_in = EC.number_of_windows_to_be(2)
        # Open new tab with specific url in case cmdclick_and_wait does not work
        try:
            SneakerScraper.cmdclick_and_wait(self.clickable_brand, self.action, self.wait, until_in)
            # Switch to new tab.
            self.driver.switch_to.window(self.driver.window_handles[1])
            self.brand_url = self.driver.current_url
            if '/en/' not in self.brand_url:
                # Insert /en/ between ".com" and "/sneakers/" 
                brand = self.brand_url
                self.brand_url = re.sub(r'(.+\.com)(/sneaker/.*)', r'\1/en\2', brand)
                print(self.brand_url)
                self.driver.get(self.brand_url)
                sleep(2)
                
        except TimeoutException:
            print('TimeoutException')
            self.driver.execute_script("window.open('');")
            self.driver.switch_to.window(self.driver.window_handles[1])
            self.driver.get(self.brand_url)
            # get url from current window
            self.brand_url = self.driver.current_url
            if '/en/' not in self.brand_url:
                # Insert /en/ between ".com" and "/sneakers/" 
                brand = self.brand_url
                self.brand_url = re.sub(r'(.+\.com)(/sneaker/.*)', r'\1/en\2', brand)
                print(self.brand_url)
                self.driver.get(self.brand_url)
                sleep(2)
        

    def jump_to_page(self, page):
        driver = self.driver
        action = self.action
        wait = self.wait

        pages_box = self.driver.find_element(By.CLASS_NAME, 'pagination')
        clickable_next_page = pages_box.find_element(By.LINK_TEXT, '›')
        href = clickable_next_page.get_attribute('href')
        # with "/en/sneaker/adidas/page/2" -> replace page/number to page/{page} with regex.
        href = re.sub(r'page/\d+', f'page/{page}', href)
        driver.get(href)
        until_in = EC.presence_of_element_located((By.TAG_NAME, 'section'))
        try:
            self.wait.until(until_in)
        except TimeoutException:
                print('TimeoutException')
                back_url = self.get_last_page()
                driver.get(back_url)
                SneakerScraper.go_back_or_forward(driver, action, wait, until_in)
        return True, page

    def next_page_action(self, pages_box, page):
        driver = self.driver
        wait = self.wait
        # TODO: Fail when not found. NoSuchElementException. not _continue
        try:
            clickable_next_page = pages_box.find_element(By.LINK_TEXT, '›')
        except NoSuchElementException:
            print('No more pages')
            _continue = False
            return _continue, page
        driver.get(clickable_next_page.get_attribute('href'))
        until_in = EC.presence_of_element_located((By.TAG_NAME, 'section'))
        wait.until(until_in)
        _continue = True
        page += 1
        return _continue, page
    
    def get_last_page(self):
        driver = self.driver
        # Get current url
        current_url = driver.current_url
        # If current url contains /page/number, then replace it with /page/number-1
        if '/page/' in current_url:
            page = int(current_url.split('/')[-1])
            current_url = re.sub(r'page/\d+', f'page/{page-1}', current_url)
            return current_url
        else:
            raise ValueError('URL has no /page/')
    
    def goto_next_page(self, page):
        driver = self.driver
        action = self.action
        wait = self.wait
        
        try:
            pages_box = driver.find_element(By.CLASS_NAME, 'pagination')
            _continue, page = self.next_page_action(pages_box, page)
        except NoSuchElementException as e:
            print(e.__class__.__name__)
            print('Need to refresh pagination.')
            driver.refresh()
            until_in = EC.presence_of_element_located((By.CLASS_NAME, 'pagination'))
            wait.until(until_in)
            pages_box = driver.find_element(By.CLASS_NAME, 'pagination')
            _continue, page = self.next_page_action(pages_box, page)
        except NoSuchElementException as e:
            print(e.__class__.__name__)
            print('No more pages')
            _continue = False
            sleep(5)
        return _continue, page

    def extract_sneakers_from_page(self, sneakers_elems, page):
        total_sneakers = len(sneakers_elems)
        brand = self.brand_name
        driver = self.driver
        sneakers = []
        # Extract page number from url and compare with page param.
        page_url = driver.current_url
        if '/page/' in page_url:
            regex_page_num = re.search(r'/page/(\d+).*', page_url)
            page_url_n = int(regex_page_num.group(1)) 
            if page != page_url_n:
                print("Processing wrong page.")
                # Replace curent_url page number from page_url to page.
                page_url = re.sub(r'page/\d+', f'page/{page}', page_url)
                driver.get(page_url)
            else:
                print(f"Processing correct page. {page}={page_url}")
        for index in range(total_sneakers):
            print(f'Processing sneaker {index+1} of {total_sneakers}')
            # Extract page number from url with regext
            self.brand_url = self.driver.current_url
            sneaker = SneakerScraper(
                driver,
                self.parent_url,
                self.wait,
                self.action,
                self.brand_name,
                sneakers_elems,
                index
            ).process_sneaker_unit()
            sneakers.append(sneaker)
            # Return to previous page.
            driver.back()
        return sneakers

    def process_sneakers_page(self):
        # Open new tab where all brand's sneakers catalogue is.
        self.open_new_tab()
        # Refresh sneakers list
        _continue = True
        page = 1
        while _continue:
            # if page in (93,94,95):
            #     page += 9
            #     continue
            # if  page == 1: # Adidas: page 161-162 possibly missing, 240-241 possibly missing
            #     # page = 31
            #     _continue, page = self.jump_to_page(page)
            sneakers_elems = self.driver.find_elements(By.TAG_NAME, 'section')
            sneakers = self.extract_sneakers_from_page(sneakers_elems, page)
            # Write sneakers information to local json file. With brand and page as name. In a brand folder.
            # if brand folder does not exist. Create it
            brand_folder = f'{self.brand_name}'
            if not os.path.exists(brand_folder):
                os.makedirs(brand_folder)
            with open(f'{brand_folder}/{self.brand_name}_{page}.json', 'w', encoding='utf-8') as file:
                file.write(json.dumps(sneakers))

            print(f"{self.brand_name} page {page} processed")
            _continue, page = self.goto_next_page(page)
