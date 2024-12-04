# Python: Fetch TomTom Traffic Incidents and store in SQLite

import requests
from datetime import datetime
import schedule
import time
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv('TOMTOM_API_KEY')

BASE_URL = 'https://api.tomtom.com/'

INCIDENTS_SERVICE = 'traffic/services'
INCIDENTS_VERSION_NUMBER = '5'
INCIDENTS_ENDPOINT = 'incidentDetails'

GEOCODING_SERVICE = 'search'
GEOCODING_VERSION_NUMBER = '2'
GEOCODING_ENDPOINT = 'geocode'

INCIDENTS_params = {
    'key': API_KEY,
    'bbox': '1.272168, 103.832378,1.293089, 103.855881', # EPSG:4326 - WGS 84 projection
    'fields' : '{incidents{type, geometry{type, coordinates}, properties{id, iconCategory, magnitudeOfDelay, events{description, code, iconCategory}, startTime, endTime, from, to, length, delay, roadNumbers, timeValidity, probabilityOfOccurrence, numberOfReports, lastReportTime, tmc{countryCode, tableNumber, tableVersion, direction ,points{location, offset}}}}}',
    'language': 'en-GB',
    'timeValidityFilter': 'present'
}

GEOCODING_params = {
    'key' : API_KEY
}

def get_bbox(location):
    query = location

    url = f"{BASE_URL}/{GEOCODING_SERVICE}/{GEOCODING_VERSION_NUMBER}/{GEOCODING_ENDPOINT}/{query}.json"
    response = requests.get(url, params=GEOCODING_params)
    if response.status_code == 200:
        data = response.json()
        if data['results']:
            position = data['results'][0]['position']
            lat = position['lat']
            lon = position['lon']
            bbox = f"{lon-0.1},{lat-0.1},{lon+0.1},{lat+0.1}"
            return bbox
        else:
            print("No results found for the location.")
            return None
    else:
        print(f"Error: {response.status_code} \n {response.text}")
        return None

def fetch_incidents(bbox):
    INCIDENTS_params['bbox'] = bbox
    url = f"{BASE_URL}/{INCIDENTS_SERVICE}/{INCIDENTS_VERSION_NUMBER}/{INCIDENTS_ENDPOINT}"
    response = requests.get(url, params=INCIDENTS_params)
    print(response.url)
    if response.status_code == 200:
        return response.json().get('incidents', [])
    else:
        print(f"Error: {response.status_code} \n {response.text}")
        return []


def initialize_db(conn):
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS incidents (
            id TEXT PRIMARY KEY,
            type TEXT,
            geometry_type TEXT,
            coordinates TEXT,
            icon_category TEXT,
            fetched_at DATETIME
        )
    ''')
    conn.commit()

def store_incidents(conn, incidents):
    cursor = conn.cursor()
    for incident in incidents:
        cursor.execute('''
            INSERT OR REPLACE INTO incidents (id, type, geometry_type, coordinates, icon_category, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            incident.get('id'),
            incident.get('type'),
            incident['geometry']['type'],
            str(incident['geometry']['coordinates']),
            incident['properties'].get('iconCategory'),
            datetime.now()
        ))
    conn.commit()

if __name__ == "__main__":
    location = "Singapore"
    bbox = get_bbox(location)
    print(bbox)
    if bbox:
        incidents = fetch_incidents(bbox)
        if incidents:
            print(len(incidents))
            print(incidents[0])
            conn = sqlite3.connect('incidents.db')
            initialize_db(conn)
            store_incidents(conn, incidents)
            conn.close()