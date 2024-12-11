import os
import json
import logging
import time
from datetime import datetime
from dotenv import load_dotenv
from TomTom_APIs import Geocode, TrafficIncidents

import csv
import schedule
import sqlite3

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

def initialize_db(dir_path):
    global conn
    db_name = f"TrafficIncidents.db"
    db_path = os.path.join(dir_path, db_name)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS incidents (
            id TEXT PRIMARY KEY,
            type TEXT,
            geometry_type TEXT,
            coordinates TEXT,
            magnitudeOfDelay REAL,
            startTime TEXT,
            endTime TEXT,
            from_location TEXT,
            to_location TEXT,
            length REAL,
            delay REAL,
            roadNumbers TEXT,
            timeValidity TEXT,
            probabilityOfOccurrence TEXT,
            numberOfReports INTEGER,
            lastReportTime TEXT,
            countryCode TEXT,
            tableNumber INTEGER,
            tableVersion INTEGER,
            direction TEXT
        )
    ''')
    conn.commit()

def insert_incident(conn, incident):
    properties = incident['properties']
    try:
        conn.execute('''
            INSERT OR REPLACE INTO incidents (
                id, type, geometry_type, coordinates, magnitudeOfDelay, startTime,
                endTime, from_location, to_location, length, delay, roadNumbers,
                timeValidity, probabilityOfOccurrence, numberOfReports, lastReportTime,
                countryCode, tableNumber, tableVersion, direction
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            properties['id'],
            incident['type'],
            incident['geometry']['type'],
            json.dumps(incident['geometry']['coordinates']),
            properties.get('magnitudeOfDelay', 0),
            properties.get('startTime'),
            properties.get('endTime'),
            properties.get('from'),
            properties.get('to'),
            properties.get('length'),
            properties.get('delay'),
            ','.join(properties.get('roadNumbers', [])),
            properties.get('timeValidity'),
            properties.get('probabilityOfOccurrence'),
            properties.get('numberOfReports', 0),
            properties.get('lastReportTime'),
            properties['tmc'].get(['countryCode']) if properties['tmc'] is not None else None,
            properties['tmc'].get(['tableNumber']) if properties['tmc'] is not None else None,
            properties['tmc'].get(['tableVersion']) if properties['tmc'] is not None else None,
            properties['tmc'].get(['direction']) if properties['tmc'] is not None else None
        ))
    except sqlite3.IntegrityError as e:
        logging.error(f"SQLite IntegrityError: {e}", exc_info=True)

def fetch_and_process(INCIDENTS_params, csv_file):
    try:
        logging.info("Starting fetch for incidents.")
        
        # Fetch incidents
        IncidentsAPI.get_incidents(INCIDENTS_params)
        
        if IncidentsAPI.incidents:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Append incidents to the current JSON Lines file
            for incident in IncidentsAPI.incidents:
                insert_incident(conn, incident)
            conn.commit()
            logging.info("Incidents inserted into SQLite DB.")
            
            # Analysis
            total_incidents = len(IncidentsAPI.incidents)
            incidents_with_delay = sum(1 for incident in IncidentsAPI.incidents if incident['properties'].get('magnitudeOfDelay', 0) > 0)
            if total_incidents > 0:
                average_delay = sum(incident['properties']['delay'] if incident['properties']['delay'] is not None else 0 for incident in IncidentsAPI.incidents) / total_incidents
            else:
                average_delay = 0

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
                elif icon_category in [7, 8, 9]: 
                    planned_works_closures += 1
                else :
                    unknown += 1

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
                    planned_works_closures,
                    unknown
                ])
            logging.info(f"Analysis logged to {csv_file}")
        else:
            logging.info("No incidents found.")
    
    except Exception as e:
        logging.error("An error occurred while fetching and processing incidents.", exc_info=True)

if __name__ == "__main__":
    location = "Singapore"
    dir_path = f"{location}_TrafficIncidents"
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
                'Unknown Causes'
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

    # Initialize the first SQLite DB file
    initialize_db(dir_path)
    
    # Initial run
    fetch_and_process(INCIDENTS_params, csv_file)

    # Schedule fetching and processing of incidents
    schedule.every(1).minutes.do(fetch_and_process, INCIDENTS_params, csv_file)
    
    while True:
        schedule.run_pending()
        time.sleep(1)