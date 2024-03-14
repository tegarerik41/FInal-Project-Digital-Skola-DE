import requests
import pandas as pd
import logging

class GetApi():
    def __init__(self, url):
        self.url = url

    def get_api_data(self):
        response = requests.get(self.url)
        result = response.json()['data']['content']
        logging.info('GET DATA FROM API COMPLETED')
        df = pd.json_normalize(result)
        logging.info('LOAD DATA TO DATAFRAME READY')
        return df

    