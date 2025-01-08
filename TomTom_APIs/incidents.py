import requests
import logging

# Define logger for module
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)  # Default logging level

# If  logger has no handlers add console handler
if not logger.hasHandlers():
    console_handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(filename)s - %(message)s')
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    logger.propagate = False

class TrafficIncidents():
    def __init__(self, API):
        self.BASE_URL = API['BASE_URL']
        self.INCIDENTS_SERVICE = API['SERVICE']
        self.INCIDENTS_VERSION_NUMBER = API['VERSION_NUMBER']
        self.INCIDENTS_ENDPOINT = API['ENDPOINT']
    logger.info("TrafficIncidents API configuration initialized.")
    
    def get_incidents(self, params):
        url = f"{self.BASE_URL}/{self.INCIDENTS_SERVICE}/{self.INCIDENTS_VERSION_NUMBER}/{self.INCIDENTS_ENDPOINT}"
        try:
            response = requests.get(url, params=params) 
            response.raise_for_status() # Raise http error if it occurred
            self.incidents = response.json().get('incidents', [])
            logger.info(f"Fetched {len(self.incidents)} incidents.")
            return self.incidents
        except requests.exceptions.RequestException as e:
            logger.error(f"An error occurred: {e}")
            self.incidents = []
            return self.incidents
