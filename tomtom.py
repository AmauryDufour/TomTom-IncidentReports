import os
import json
import sqlite3
import logging
import time
from datetime import datetime
from dotenv import load_dotenv
from TomTom_APIs import Geocode, TrafficIncidents

import csv

import schedule


logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("tomtom.log"),
    ]
)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logging.getLogger().addHandler(console)

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
            json_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            xslx_timestamp = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
            
            data = IncidentsAPI.incidents_response.json()
            json_file = os.path.join(dir_path, f"{json_timestamp}.json")
            with open(json_file, "w") as outfile:
                json.dump(data, outfile, indent=4)
            logging.info(f"Incidents saved to {json_file}")
            
            # Analysis
            total_incidents = len(IncidentsAPI.incidents)
            incidents_with_delay = sum(1 for incident in IncidentsAPI.incidents if incident['properties'].get('magnitudeOfDelay', 0) > 0)
            average_delay = sum(incident['properties'].get('magnitudeOfDelay', 0) for incident in IncidentsAPI.incidents) / total_incidents if total_incidents > 0 else 0

            # Incident Distribution by Type
            environmental_causes = 0
            accidents_breakdowns = 0
            jams = 0
            planned_works_closures = 0
            unknown = 0

            for incident in IncidentsAPI.incidents:
                icon_category = incident['properties'].get('iconCategory', 0)
                if icon_category in [2, 3, 4, 5, 10, 11]:
                    environmental_causes += 1
                elif icon_category in [1, 14]:
                    accidents_breakdowns += 1
                elif icon_category == 0:
                    unknown += 1
                elif icon_category == 6:
                    jams += 1
                else:
                    planned_works_closures += 1

            with open(csv_file, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    xslx_timestamp, 
                    total_incidents, 
                    incidents_with_delay, 
                    round(average_delay, 2),
                    environmental_causes,
                    accidents_breakdowns,
                    planned_works_closures,
                    jams, 
                    unknown
                ])

            logging.info(f"Report logged to {csv_file}")
        else:
            logging.info("No incidents found.")
    
    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == "__main__":

    location = "Singapore"
    logging.info(f"Location set: {location}")
    dir_path = f"{location}_Incidents"
    os.makedirs(dir_path, exist_ok=True)
    logging.info(f"Directory created: {dir_path}")
    csv_file = os.path.join(dir_path, 'report.csv')
    if not os.path.exists(csv_file):
        logging.info(f"Creating report CSV file: {csv_file}")
        with open(csv_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                'Timestamp', 
                'Total Incidents', 
                'Incidents With Delay', 
                'Average Delay',
                'Environmental Causes',
                'Accidents & Breakdowns',
                'Planned Works & Closures',
                'Jams',
                'Unknown Causes'
            ])

    Geocode_API = Geocode(GEOCODING_API_URLS)
    IncidentsAPI = TrafficIncidents(TRAFFIC_INCIDENTS_API_URLS)
    
    # Get and reformat bounding box
    logging.info("Fetching Bounding Box.")
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