# Python: Fetch TomTom Traffic Incidents, store in JSON, perform analysis, and log to CSV

import os
import json
import sqlite3
import logging
import time
from datetime import datetime
from dotenv import load_dotenv
from utils import Geocode, TrafficIncidents

import csv

import schedule
logging.basicConfig(
    filename='tomtom.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

load_dotenv()
API_KEY = os.getenv('TOMTOM_API_KEY')
BASE_URL = os.getenv('BASE_URL')

TRAFFIC_INCIDENTS_API_URLS = json.loads(os.getenv('TRAFFIC_INCIDENTS_API_URLS'))
GEOCODING_API_URLS = json.loads(os.getenv('GEOCODING_API_URLS'))

# Initialize Parameters
INCIDENTS_params = {
    'key': API_KEY,
    'bbox': '',
    'fields': '{incidents{type,geometry{type,coordinates},properties{id,iconCategory,magnitudeOfDelay,events{description,code,iconCategory},startTime,endTime,from,to,length,delay,roadNumbers,timeValidity,probabilityOfOccurrence,numberOfReports,lastReportTime,tmc{countryCode,tableNumber,tableVersion,direction,points{location,offset}}}}}',
    'language': 'en-GB',
    'timeValidityFilter': 'present'
}

GEOCODING_params = {
    'key': API_KEY,
}

def fetch_and_process(INCIDENTS_params, dir_path, csv_file):
    try:
        logging.info("Starting fetch for incidents.")
        
        # Fetch incidents
        IncidentsAPI.get_incidents(INCIDENTS_params)
        
        if IncidentsAPI.incidents:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            data = IncidentsAPI.incidents_response.json()
            json_file = os.path.join(dir_path, f"{timestamp}.json")
            with open(json_file, "w") as outfile:
                json.dump(data, outfile, indent=4)
            logging.info(f"Incidents saved to {json_file}")
            
            # Analysis
            total_incidents = len(IncidentsAPI.incidents)
            incidents_with_delay = sum(1 for incident in IncidentsAPI.incidents if incident['properties'].get('magnitudeOfDelay', 0) > 0)
            average_delay = sum(incident['properties'].get('magnitudeOfDelay', 0) for incident in IncidentsAPI.incidents) / total_incidents if total_incidents > 0 else 0
            
            with open(csv_file, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([timestamp, total_incidents, incidents_with_delay, round(average_delay, 2)])
            logging.info(f"Analysis logged to {csv_file}")
        else:
            logging.info("No incidents found.")
    
    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == "__main__":

    location = "Singapore"
    dir_path = f"{location}_Incidents"
    os.makedirs(dir_path, exist_ok=True)

    csv_file = os.path.join(dir_path, 'analysis.csv')
if not os.path.exists(csv_file):
    with open(csv_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Timestamp', 'Total_Incidents', 'Incidents_With_Delay', 'Average_Delay'])
    
    Geocode_API = Geocode(GEOCODING_API_URLS)
    IncidentsAPI = TrafficIncidents(TRAFFIC_INCIDENTS_API_URLS)
    
    # Get and reformat bounding box
    bbox = Geocode_API.get_bbox(GEOCODING_params, location)
    if bbox:
        reformatted_bbox = Geocode_API.reformatbbox(bbox)
        INCIDENTS_params['bbox'] = reformatted_bbox
        logging.info(f"Reformatted BBox: {reformatted_bbox}")
    else:
        logging.error("Failed to retrieve bounding box.")
        exit()
    logging.info("TomTom Incident Fetcher Started.")

    fetch_and_process(INCIDENTS_params, dir_path, csv_file)  # Initial run

    # Schedule to run every minute
    schedule.every(1).minutes.do(fetch_and_process, INCIDENTS_params, dir_path, csv_file)
    while True:
        schedule.run_pending()
        time.sleep(1)