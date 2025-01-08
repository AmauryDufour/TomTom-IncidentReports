import os
import logging
import csv
from datetime import datetime

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

class csvReport:
    def __init__(self, 
                 dir_path, 
                 headers = ['Timestamp', 
                            'Current Incidents', 
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
            logger.info(f"Created CSV file with headers: {self.csv_path}")

    def analyse_commit(self, incidents, changes, inserts):
            # Analysis
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            total_incidents = len(incidents)
            incidents_with_delay = sum(1 for incident in incidents if (incident['properties'].get('delay', 0) or 0) > 0)
            total_delay = sum(incident['properties'].get('delay', 0) or 0 for incident in incidents)
            average_delay = total_delay / incidents_with_delay if incidents_with_delay > 0 else 0

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
                else:
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
            logger.info(f"Stats logged to {self.csv_path}")
