import requests
import logging

class TrafficIncidents():
    def __init__(self, API):
        self.BASE_URL = API['BASE_URL']
        self.INCIDENTS_SERVICE = API['SERVICE']
        self.INCIDENTS_VERSION_NUMBER = API['VERSION_NUMBER']
        self.INCIDENTS_ENDPOINT = API['ENDPOINT']
    
    def get_incidents(self, params):
        url = f"{self.BASE_URL}/{self.INCIDENTS_SERVICE}/{self.INCIDENTS_VERSION_NUMBER}/{self.INCIDENTS_ENDPOINT}"
        self.incidents_response = requests.get(url, params=params)
        if self.incidents_response.status_code == 200:
            self.incidents = self.incidents_response.json().get('incidents', [])
            return self.incidents
        else:
            logging.error(f"An error occurred: {self.incidents_response.status_code} - {self.incidents_response.text}")
            self.incidents = []
            return self.incidents
