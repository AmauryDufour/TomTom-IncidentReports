# Python: Fetch TomTom Traffic Incidents and store in SQLite

import os
import json
from dotenv import load_dotenv

from utils import Geocode, TrafficIncidents

load_dotenv()
API_KEY = os.getenv('TOMTOM_API_KEY')
BASE_URL = os.getenv('BASE_URL')

TRAFFIC_INCIDENTS_API_URLS = json.loads(os.getenv('TRAFFIC_INCIDENTS_API_URLS'))
GEOCODING_API_URLS = json.loads(os.getenv('GEOCODING_API_URLS'))

INCIDENTS_params = {
    'key': API_KEY,
    'bbox': '', # EPSG:4326 - WGS 84 projection "min_lon,min_lat,max_lon,max_lat"
    'fields' : '{incidents{type,geometry{type,coordinates},properties{id,iconCategory,magnitudeOfDelay,events{description,code,iconCategory},startTime,endTime,from,to,length,delay,roadNumbers,timeValidity,probabilityOfOccurrence,numberOfReports,lastReportTime,tmc{countryCode,tableNumber,tableVersion,direction,points{location,offset}}}}}',
    'language': 'en-GB',
    'timeValidityFilter': 'present'
}

GEOCODING_params = {
    'key' : API_KEY,
}

if __name__ == "__main__":
    location = "Singapore"

    Geocode_API = Geocode(GEOCODING_API_URLS)
    IncidentsAPI = TrafficIncidents(TRAFFIC_INCIDENTS_API_URLS)
    
    INCIDENTS_params['bbox'] = Geocode_API.reformatbbox(Geocode_API.get_bbox(GEOCODING_params, location))
    IncidentsAPI.get_incidents(INCIDENTS_params)

    if IncidentsAPI.incidents:
        print(len(IncidentsAPI.incidents))
        with open('incidents.json', 'w') as f:
            json.dump(IncidentsAPI.incidents_response.json(), f, indent=4)
