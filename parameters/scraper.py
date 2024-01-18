from abc import ABC, abstractmethod
import requests

class Scraper(ABC):

    def __init__(self):
        self.url = ""
        self.payload = {}
        self.headers = {}
    
    def get_request(self, **kwargs):
        """
        Updates necessary values in [self.payload] and gets the request.
        """
        self.payload.update(kwargs)
        self.response = requests.get(self.url, params=self.payload, headers=self.headers)
    
    @abstractmethod
    def extract(self, json, **kwargs):
        """
        Given a JSON object, extract and return the necessary data.
        """

    def forward(self, **kwargs):
        if self.response.status_code == 200:
            return self.response.json()

        else: # method
            raise Exception(f"Error: {self.response.status_code}")