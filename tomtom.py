import os
import json
import logging
import logging.config
import time
from datetime import datetime, timedelta, UTC
import schedule

from dotenv import load_dotenv

from TomTom_APIs import Geocode, TrafficIncidents
from utils import TrafficIncidentsDB, csvReport

logging_config = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s [%(levelname)s] %(message)s'
        },
        'detailed': {
            'format': '%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d - %(message)s'
        },
    },
    'handlers': {
        'file_handler_error': {
            'class': 'logging.handlers.RotatingFileHandler',
            'level': 'WARNING',
            'formatter': 'detailed',
            'filename': 'logs/error_log.log',
            'maxBytes': 5*1024*1024,
            'backupCount': 5,
        },
        'file_handler_debug': {
            'class': 'logging.handlers.RotatingFileHandler',
            'level': 'DEBUG',
            'formatter': 'detailed',
            'filename': 'logs/debug_log.log',
            'maxBytes': 5*1024*1024,
            'backupCount': 5,
        },
        'console_handler': {
            'class': 'logging.StreamHandler',
            'level': 'INFO',
            'formatter': 'standard',
        },
    },
    'root': {
        'handlers': ['file_handler_error', 'file_handler_debug', 'console_handler'],
        'level': 'DEBUG',
    },
}

logging.config.dictConfig(logging_config)
logger = logging.getLogger(__name__)
logger.debug("Logging is configured.")

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

def fetch_and_process(INCIDENTS_params, csv_file, database, threshold_minutes=5):
    try:
        logger.info("Starting fetch for incidents.")
        
        # Fetch incidents
        IncidentsAPI.get_incidents(INCIDENTS_params)
        
        if IncidentsAPI.incidents:
            
            # Append new incidents to the db and update those that have changed
            changes, inserts = database.update_incidents(IncidentsAPI.incidents)

            # Analysis
            csv_file.analyse_commit(IncidentsAPI.incidents, changes, inserts) 
            
            # Mark ended incidents
            database.mark_ended_incidents(threshold_minutes=threshold_minutes)
 
        else:
            logger.info("No incidents found.")
    
    except Exception as e:
        logger.error("An error occurred while fetching and processing incidents.", exc_info=True)

if __name__ == "__main__":
    location = "Singapore"
    dir_path = f"{location}_TrafficIncidents"
    os.makedirs(dir_path, exist_ok=True)
    
    Geocode_API = Geocode(GEOCODING_API_URLS)
    IncidentsAPI = TrafficIncidents(TRAFFIC_INCIDENTS_API_URLS)

    # Get and reformat bounding box
    logger.info("Starting Geocoding.")
    bbox = Geocode_API.get_bbox(GEOCODING_params, location)
    if bbox:
        reformatted_bbox = Geocode_API.reformatbbox(bbox)
        INCIDENTS_params['bbox'] = reformatted_bbox
        logger.info(f"Formatted BBox (min_lon,min_lat,max_lon,max_lat): ({reformatted_bbox})")
    else:
        logger.error("Failed to retrieve bounding box.")
        exit()
    logger.info("TomTom Incident Fetcher Started.")

    # Initialize the SQLite DataBase
    db = TrafficIncidentsDB(dir_path, location=location)

    # Initialize the report
    report = csvReport(dir_path)

    fetch_and_process(INCIDENTS_params=INCIDENTS_params, csv_file=report, database=db, threshold_minutes=5)

    # Schedule fetching and processing of incidents
    schedule.every(40).seconds.do(fetch_and_process, INCIDENTS_params=INCIDENTS_params, csv_file=report, database=db, threshold_minutes=5)
    schedule.every(1).day.at('12:30:00').do(db.optimize)

    while True:
        schedule.run_pending()
        time.sleep(1)