import requests


class Geocode():
    def __init__(self, API):
        self.BASE_URL = API['BASE_URL']
        self.GEOCODING_SERVICE = API['SERVICE']
        self.GEOCODING_VERSION_NUMBER = API['VERSION_NUMBER']
        self.GEOCODING_ENDPOINT = API['ENDPOINT']

    def get_bbox(self, params, location):
        url = f"{self.BASE_URL}/{self.GEOCODING_SERVICE}/{self.GEOCODING_VERSION_NUMBER}/{self.GEOCODING_ENDPOINT}/{location}.json"
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            if data['results']:
                max_confidence_index = max(range(len(data['results'])), key=lambda i: data['results'][i]['matchConfidence']['score'])
                while data['results'][max_confidence_index]['type'] != 'Geography':
                    data['results'].pop(max_confidence_index)
                    max_confidence_index = max(range(len(data['results'])), key=lambda i: data['results'][i]['matchConfidence']['score'])
                    if not data['results']:
                        print("No results found for the location.")
                        return None
                self.bbox = data['results'][max_confidence_index]['boundingBox']
                return self.bbox
            
            else:
                print("No results found for the location.")
                return None
        else:
            print(f"Error: {response.status_code} \n {response.text}")
            return None
        
    def reformatbbox(self, bbox = None):
        if not bbox : bbox = self.bbox
        min_lon = min(bbox['topLeftPoint']['lon'],bbox['btmRightPoint']['lon'])
        max_lon = max(bbox['topLeftPoint']['lon'],bbox['btmRightPoint']['lon'])
        min_lat = min(bbox['topLeftPoint']['lat'],bbox['btmRightPoint']['lat'])
        max_lat = max(bbox['topLeftPoint']['lat'],bbox['btmRightPoint']['lat'])
        return f"{min_lon},{min_lat},{max_lon},{max_lat}"