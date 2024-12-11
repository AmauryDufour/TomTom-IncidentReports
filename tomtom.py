import os
import json
import logging
import time
from datetime import datetime
from dotenv import load_dotenv
from TomTom_APIs import Geocode, TrafficIncidents

import csv
import schedule

# Global variable to hold the current JSON Lines file path
current_jsonl_file = None

# Updated Logging Configuration with Line Numbers
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d %(message)s",
    handlers=[
        logging.FileHandler("tomtom.log"),
    ]
)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d %(message)s"))
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

def rotate_jsonl_file(dir_path):
    """
    Determines the current 12-hour window and updates the global JSON Lines file path.
    Schedules the next rotation at the next 12-hour boundary.
    """
    global current_jsonl_file
    now = datetime.now()
    date_str = now.strftime("%Y%m%d")
    time_period = 0 if now.hour < 12 else 12
    file_name = f"incidents_{date_str}_{time_period}.jsonl"
    current_jsonl_file = os.path.join(dir_path, file_name)
    logging.info(f"Switched to new JSON Lines file: {current_jsonl_file}")

def fetch_and_process(INCIDENTS_params, csv_file):
    try:
        logging.info("Starting fetch for incidents.")
        
        # Fetch incidents
        IncidentsAPI.get_incidents(INCIDENTS_params)
        
        if IncidentsAPI.incidents:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Ensure current_jsonl_file is set
            if current_jsonl_file is None:
                logging.error("JSON Lines file is not initialized.")
                return
            
            # Append incidents to the current JSON Lines file
            with open(current_jsonl_file, "a") as jf:
                for incident in IncidentsAPI.incidents:
                    jf.write(json.dumps(incident) + "\n")
            logging.info(f"Incidents appended to {current_jsonl_file}")
            
            # Analysis
            total_incidents = len(IncidentsAPI.incidents)
            incidents_with_delay = sum(1 for incident in IncidentsAPI.incidents if incident['properties'].get('magnitudeOfDelay', 0) > 0)
            average_delay = sum(incident['properties'].get('magnitudeOfDelay', 0) for incident in IncidentsAPI.incidents) / total_incidents if total_incidents > 0 else 0

            # Incident Distribution by Type
            environmental_causes = 0
            human_car_breakdowns = 0
            planned_works_closures = 0
            jams = 0
            unknown = 0

            for incident in IncidentsAPI.incidents:
                icon_category = incident['properties'].get('iconCategory', 0)
                if icon_category in [2, 3, 4, 5, 10, 11]:
                    environmental_causes += 1
                elif icon_category in [1, 14]:
                    human_car_breakdowns += 1
                elif icon_category == 6:
                    jams += 1
                else:
                    planned_works_closures += 1

            with open(csv_file, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    timestamp, 
                    total_incidents, 
                    incidents_with_delay, 
                    round(average_delay, 2),
                    environmental_causes,
                    human_car_breakdowns,
                    jams,
                    planned_works_closures
                ])
            logging.info(f"Analysis logged to {csv_file}")
        else:
            logging.info("No incidents found.")
    
    except Exception as e:
        logging.error("An error occurred while fetching and processing incidents.", exc_info=True)

if __name__ == "__main__":
    location = "Singapore"
    dir_path = f"{location}_TrafficIncidents_jsonl"
    os.makedirs(dir_path, exist_ok=True)
    
    csv_file = os.path.join(dir_path, 'report.csv')
    if not os.path.exists(csv_file):
        with open(csv_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                'Timestamp', 
                'Total_Incidents', 
                'Incidents_With_Delay', 
                'Average_Delay',
                'Environmental_Causes',
                'Human_Car_Breakdowns',
                'Jams',
                'Planned_Works_Closures',
            ])
        logging.info(f"Created CSV file with headers: {csv_file}")
    
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

    # Initialize the current JSON Lines file
    rotate_jsonl_file(dir_path)
    
    # Schedule JSON Lines file rotations
    schedule.every().day.at("00:00").do(rotate_jsonl_file, dir_path=dir_path)
    schedule.every().day.at("12:00").do(rotate_jsonl_file, dir_path=dir_path)
    logging.info("Scheduled JSON Lines file rotations at 00:00 and 12:00 daily.")
    
    # Initial run
    fetch_and_process(INCIDENTS_params, csv_file)

    # Schedule fetching and processing of incidents
    schedule.every(1).minutes.do(fetch_and_process, INCIDENTS_params, csv_file)
    
    while True:
        schedule.run_pending()
        time.sleep(1)  # Wait for 1 minute