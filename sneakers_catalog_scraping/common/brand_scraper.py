from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support import expected_conditions as EC
from common.model_scraper import ModelScraper
from common.sneaker_page_scraper import SneakerPageScraper
from selenium.webdriver.common.keys import Keys

class BrandScraper():

    def __init__(self, driver, parent_url, wait, action):
        self.driver = driver
        self.parent_url = parent_url
        self.wait = wait
        self.action = action
        self.brands_divs = self.get_brands_divs()

    def get_brands_divs(self):
        parent_div = self.driver.find_element(By.XPATH, '//*[@id="__layout"]/div/div/div[1]/article/div')
        brands_divs = parent_div.find_elements(By.TAG_NAME, 'div')
        return brands_divs
    def close_current_tab(self, until_in):
        driver = self.driver
        wait = self.wait

        driver.close()
        wait.until(until_in)
        driver.switch_to.window(self.driver.window_handles[0])

    def refresh_brand_divs(self):
        # Close current tap and refresh.
        until_in = EC.number_of_windows_to_be(1)
        self.close_current_tab(until_in)

        driver = self.driver
        wait = self.wait
        try:
            self.brands_divs[0].find_element(By.TAG_NAME, 'h2')
        except NoSuchElementException as e:
            print("Refreshing brands list...")
            print(e.__class__.__name__)
            driver.refresh()
            wait.until(
                EC.presence_of_element_located((By.TAG_NAME, 'section'))
            )
            parent_div = driver.find_element(By.XPATH, '//*[@id="__layout"]/div/div/div[1]/article/div')
            self.brands_divs = parent_div.find_elements(By.TAG_NAME, 'div')
            # sleep(5)
        return self.brands_divs

    # Process brand function
    def process_brands(self):
        brands = {
            'brands': [],
            'total_brands': 0
        }
        brand_divs_n = len(self.brands_divs)
        cnt = 1
        for index in range(brand_divs_n):
            # if cnt > 1:
            #     self.refresh_brand_divs()
            current_brand = self.brands_divs[index]
            if BrandScraper.has_specific_element(current_brand):
                break
            brand = {
                'name' : None,
                'url' : None,
                'brand_count' : 0,
                'models' : []
            }

            brand_h2 = current_brand.find_element(By.TAG_NAME, 'h2')
            brand_a = brand_h2.find_element(By.TAG_NAME, 'a')
            brand_name = brand_a.text
            brand['name'] = brand_name
            # if brand_name in ['Adidas', 'Jordan', 'Converse', 'Nike',
            #  'Asics', 'New Balance', 'Vans', 'Saucony', 'Puma', 'Reebok', 'Clarks',
            #  'Timberland', 'Diadora', 'Veja', 'On', 'Autry Action Shoes',
            #  'Alexander Mcqueen', 'Balenciaga', 'Dr Martens',
            #  'Mizuno','Lacoste','Kangaroos', 'Le Coq Sportif', 'Karhu', 'Birkenstock',
            #  'Suicoke','Camper', 'Off White', 'Hoka', 'Adidas Originals', 'Adidas Neo',
            #   'Ewing', 'New Balance Kg', 'Nike Kg', 'Jordan Kg',
            #   'Amiri', 'Salomon', 'Bape', 'Lanvin',
            #   'Onitsuka Tiger']: 
            #     continue
            brand['url'] = brand_a.get_attribute('href')
            brand['brand_count'] = ModelScraper.get_total_from_elem(brand_h2,'brand-count')

            models = ModelScraper(self.driver,
                self.parent_url,
                self.wait,
                self.action,
                current_brand
                ).process_models()

            brand['models'] = brand['models'] + models
            brands['brands'].append(brand)
            brands['total_brands'] += 1

            # SneakerPageScraper(
            #     self.driver,
            #     self.parent_url,
            #     self.wait,
            #     self.action,
            #     brand_a
            # ).process_sneakers_page()
            cnt +=1 
            print(f"brand {brand['name']} processed. {round(((index+1)/len(self.brands_divs))*100,2)}% of progress.")
        return brands

    # Static functions
    @staticmethod
    def has_specific_element(parent_elem):
        try:
            parent_elem.find_element(By.CSS_SELECTOR, '#__layout > div > div > div:nth-child(2) > article > div > div:nth-child(45) > div > div:nth-child(1)')
            resp = True
        except Exception:
            resp = False
        return resp
    
    