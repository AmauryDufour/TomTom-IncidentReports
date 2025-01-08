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


class Geocode():
    def __init__(self, API):
        self.BASE_URL = API['BASE_URL']
        self.GEOCODING_SERVICE = API['SERVICE']
        self.GEOCODING_VERSION_NUMBER = API['VERSION_NUMBER']
        self.GEOCODING_ENDPOINT = API['ENDPOINT']
        logger.info("Geocoding API configuration initialized.")

    def get_bbox(self, params, location):
        url = f"{self.BASE_URL}/{self.GEOCODING_SERVICE}/{self.GEOCODING_VERSION_NUMBER}/{self.GEOCODING_ENDPOINT}/{location}.json"
        try :
            response = requests.get(url, params=params)
            response.raise_for_status() # Raise http error if it occurred
            data = response.json()
            if data['results']:
                max_confidence_index = max(range(len(data['results'])), key=lambda i: data['results'][i]['matchConfidence']['score'])
                while data['results'][max_confidence_index]['type'] != 'Geography':
                    data['results'].pop(max_confidence_index)
                    max_confidence_index = max(range(len(data['results'])), key=lambda i: data['results'][i]['matchConfidence']['score'])
                    if not data['results']:
                        logger.error("No results of suitable type found for the location.")
                        return None
                self.bbox = data['results'][max_confidence_index]['boundingBox']
                logger.info(f"Bounding box found: {self.bbox}")
                return self.bbox
            else:
                logger.error("No results found for the location.")
                return None
        except requests.exceptions.RequestException as e:
            logger.error(f"An error occurred: {e}")
            return None

    def reformatbbox(self, bbox=None):
        if not bbox:
            bbox = self.bbox
        min_lon = min(bbox['topLeftPoint']['lon'], bbox['btmRightPoint']['lon'])
        max_lon = max(bbox['topLeftPoint']['lon'], bbox['btmRightPoint']['lon'])
        min_lat = min(bbox['topLeftPoint']['lat'], bbox['btmRightPoint']['lat'])
        max_lat = max(bbox['topLeftPoint']['lat'], bbox['btmRightPoint']['lat'])
        reformatted_bbox = f"{min_lon},{min_lat},{max_lon},{max_lat}"
        logger.info(f"Reformatted bounding box: {reformatted_bbox}")
        return reformatted_bbox