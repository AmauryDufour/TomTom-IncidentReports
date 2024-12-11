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
console.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(filename)s - %(message)s"))
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

class csvReport:
    def __init__(self, 
                 dir_path, 
                 headers = ['Timestamp', 
                            'Total Incidents', 
                            'Incidents with Delay', 
                            'Total Delay',
                            'Average Delay',
                            'Environmental Causes',
                            'Human Car Breakdowns',
                            'Jams',
                            'Planned Works Closures',
                            'Unknown Causes',
                            'Changes', 
                            'New Incidents']):
        self.dir_path = dir_path
        self.headers = headers
        self.csv_path = os.path.join(self.dir_path, 'report.csv')
        if not os.path.exists(self.csv_path):
            with open(self.csv_path, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(self.headers)
            logging.info(f"Created CSV file with headers: {self.csv_path}")

    def analyse_commit(self, incidents, changes, inserts):
            # Analysis
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            total_incidents = len(incidents)
            incidents_with_delay = sum(1 for incident in incidents if incident['properties'].get('delay', 0) or 0 > 0)
            total_delay = sum(incident['properties'].get('delay', 0) or 0 for incident in incidents)
            average_delay = total_delay / incidents_with_delay

            # Incident Distribution by Type
            environmental_causes = 0
            human_car_breakdowns = 0
            planned_works_closures = 0
            jams = 0
            unknown = 0

            for incident in incidents:
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

            with open(self.csv_path, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    timestamp, 
                    total_incidents, 
                    incidents_with_delay, 
                    total_delay,
                    average_delay,
                    environmental_causes,
                    human_car_breakdowns,
                    jams,
                    planned_works_closures,
                    unknown,
                    changes,
                    inserts
                ])
            logging.info(f"Stats logged to {self.csv_path}")

class TrafficIncidentsDB:
    def __init__(self, dir_path):
        self.db_path = os.path.join(dir_path, "TrafficIncidents.db")
        self.conn = sqlite3.connect(self.db_path)
        self.initialize_db()

    def initialize_db(self):
        cursor = self.conn.cursor()
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
        self.conn.commit()

    def insert_incident(self, incident):
        properties = incident['properties']
        incident_id = properties['id']
        new_delay = properties.get('delay') or 0  # Treat None as 0
        
        try:
            # Check existing delay
            cursor = self.conn.cursor()
            cursor.execute('SELECT delay FROM incidents WHERE id = ?', (incident_id,))
            row = cursor.fetchone()
            
            if row:
                current_delay = row[0] or 0  # Treat None as 0
                if new_delay > current_delay:
                    # Update row with new data if new delay is greater
                    cursor.execute('''
                        INSERT OR REPLACE INTO incidents (
                            id, type, geometry_type, coordinates, magnitudeOfDelay, startTime,
                            endTime, from_location, to_location, length, delay, roadNumbers,
                            timeValidity, probabilityOfOccurrence, numberOfReports, lastReportTime,
                            countryCode, tableNumber, tableVersion, direction
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        incident_id,
                        incident['type'],
                        incident['geometry']['type'],
                        json.dumps(incident['geometry']['coordinates']),
                        properties.get('magnitudeOfDelay', 0),
                        properties.get('startTime'),
                        properties.get('endTime'),
                        properties.get('from'),
                        properties.get('to'),
                        properties.get('length'),
                        new_delay,
                        ','.join(properties.get('roadNumbers', [])),
                        properties.get('timeValidity'),
                        properties.get('probabilityOfOccurrence'),
                        properties.get('numberOfReports', 0),
                        properties.get('lastReportTime'),
                        properties['tmc'].get('countryCode') if properties.get('tmc') else None,
                        properties['tmc'].get('tableNumber') if properties.get('tmc') else None,
                        properties['tmc'].get('tableVersion') if properties.get('tmc') else None,
                        properties['tmc'].get('direction') if properties.get('tmc') else None
                    ))
                    self.conn.commit()
                    return True, False
                else:
                    return False, False
            else:
                # Insert new row if incident does not exist
                cursor.execute('''
                    INSERT OR REPLACE INTO incidents (
                        id, type, geometry_type, coordinates, magnitudeOfDelay, startTime,
                        endTime, from_location, to_location, length, delay, roadNumbers,
                        timeValidity, probabilityOfOccurrence, numberOfReports, lastReportTime,
                        countryCode, tableNumber, tableVersion, direction
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    incident_id,
                    incident['type'],
                    incident['geometry']['type'],
                    json.dumps(incident['geometry']['coordinates']),
                    properties.get('magnitudeOfDelay', 0),
                    properties.get('startTime'),
                    properties.get('endTime'),
                    properties.get('from'),
                    properties.get('to'),
                    properties.get('length'),
                    new_delay,
                    ','.join(properties.get('roadNumbers', [])),
                    properties.get('timeValidity'),
                    properties.get('probabilityOfOccurrence'),
                    properties.get('numberOfReports', 0),
                    properties.get('lastReportTime'),
                    properties['tmc'].get('countryCode') if properties.get('tmc') else None,
                    properties['tmc'].get('tableNumber') if properties.get('tmc') else None,
                    properties['tmc'].get('tableVersion') if properties.get('tmc') else None,
                    properties['tmc'].get('direction') if properties.get('tmc') else None
                ))
                self.conn.commit()
                return True, True
        except sqlite3.IntegrityError as e:
            logging.error(f"SQLite IntegrityError: {e}", exc_info=True)
            return False, False
        except Exception as e:
            logging.error(f"Unexpected error: {e}", exc_info=True)
            return False, False

    def update_incidents(self, incidents):
        changes = 0
        inserts = 0
        for incident in incidents:
            changed, inserted = self.insert_incident(incident)
            if changed:
                changes += 1
            if inserted:
                inserts += 1
        self.conn.commit()
        logging.info(f"{inserts} new incident(s) inserted of {changes} changes to DB (of {len(incidents)} current).")
        return changes, inserts
    def close(self):
        self.conn.close()

def fetch_and_process(INCIDENTS_params, csv_file, db):
    try:
        logging.info("Starting fetch for incidents.")
        
        # Fetch incidents
        IncidentsAPI.get_incidents(INCIDENTS_params)
        
        if IncidentsAPI.incidents:
            
            # Append new incidents to the db and update those that have changed
            changes, inserts = db.update_incidents(IncidentsAPI.incidents)

            # Analysis
            csv_file.analyse_commit(IncidentsAPI.incidents, changes, inserts) 
            
        else:
            logging.info("No incidents found.")
    
    except Exception as e:
        logging.error("An error occurred while fetching and processing incidents.", exc_info=True)

if __name__ == "__main__":
    location = "Singapore"
    dir_path = f"{location}_TrafficIncidents"
    os.makedirs(dir_path, exist_ok=True)
    
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

    # Initialize the SQLite DB file
    db = TrafficIncidentsDB(dir_path)

    report = csvReport(dir_path)
    
    # Initial run
    fetch_and_process(INCIDENTS_params, report, db)

    # Schedule fetching and processing of incidents
    schedule.every(1).minutes.at(':30').do(fetch_and_process, INCIDENTS_params, report, db)
    
    while True:
        schedule.run_pending()
        time.sleep(1)