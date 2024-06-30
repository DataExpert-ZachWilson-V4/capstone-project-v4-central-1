from selenium.webdriver.common.by import By

class ModelScraper():
    
    def __init__(self, driver, parent_url, wait, action, current_brand):
        self.driver = driver
        self.parent_url = parent_url
        self.wait = wait
        self.action = action
        self.current_brand = current_brand
        self.models_list = self.get_models_list()

    def get_models_list(self):
        models_list = self.current_brand.find_element(By.TAG_NAME, 'ul')
        return models_list

    # Process Model function
    def process_models(self):
        _models = self.models_list.find_elements(By.TAG_NAME, 'li')
        models = []
        for model in _models:
            _model = {
                'model_name': None,
                'model_url': None,
                'model_total': 0
            }
            model_a = model.find_element(By.TAG_NAME, 'a')
            _model['model_name'] = model_a.text
            _model['model_url'] = model_a.get_attribute('href')
            _model['model_total'] = ModelScraper.get_total_from_elem(model, 'cat-count')
            models.append(_model)
        return models
    
    @staticmethod
    def get_total_from_elem(element, class_name):
        try:
            total = element.find_element(By.CLASS_NAME, class_name).text
            total = total if total != '' else 0
        except Exception as e:
            print(e.__class__.__name__)
            total = 0
        return int(total)