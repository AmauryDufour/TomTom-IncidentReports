import os
import logging
import json
import geojson
from datetime import datetime, timedelta, UTC
import sqlite3


class TrafficIncidentsDB:
    def __init__(self, dir_path, db_path = None):
        self.dir_path = dir_path
        if not db_path : self.db_path = os.path.join(self.dir_path, "TrafficIncidents.db")
        else : self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path)
        self.initialize_db()
        logging.info(f"Connection to {self.db_path} database established")

    def initialize_db(self):
        cursor = self.conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS incidents (
                id TEXT PRIMARY KEY,
                type TEXT,
                category TEXT,
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
                direction TEXT,
                last_seen TEXT
            )
        ''')
        self.conn.commit()

    def insert_incident(self, incident):
        properties = incident['properties']
        incident_id = properties['id']
        new_delay = properties.get('delay') or 0  # None as 0
        current_time = datetime.now(UTC)

        try:
            # Check existing delay and endTime
            cursor = self.conn.cursor()
            cursor.execute('SELECT delay, endTime FROM incidents WHERE id = ?', (incident_id,))
            row = cursor.fetchone()

            if row:
                current_delay = row[0] or 0  # None as 0
                current_endTime = row[1]

                if new_delay > current_delay:
                    # Determine the most recent endTime
                    provided_endTime = properties.get('endTime')
                    if provided_endTime:
                        if current_endTime:
                            provided_end_dt = datetime.fromisoformat(provided_endTime)
                            current_end_dt = datetime.fromisoformat(current_endTime)
                            end_time = max(provided_end_dt, current_end_dt).isoformat()
                        else:
                            end_time = provided_endTime
                    else:
                        end_time = current_endTime  # No new endTime provided

                    # Update incident with new delay and endTime
                    cursor.execute('''
                        UPDATE incidents
                        SET type = ?, category = ?, geometry_type = ?, coordinates = ?, magnitudeOfDelay = ?,
                            startTime = ?, endTime = ?, from_location = ?, to_location = ?, length = ?,
                            delay = ?, roadNumbers = ?, timeValidity = ?, probabilityOfOccurrence = ?,
                            numberOfReports = ?, lastReportTime = ?, countryCode = ?, tableNumber = ?,
                            tableVersion = ?, direction = ?, last_seen = ?
                        WHERE id = ?
                    ''', (
                        incident['type'],
                        properties.get('iconCategory', 0),
                        incident['geometry']['type'],
                        json.dumps(incident['geometry']['coordinates']),
                        properties.get('magnitudeOfDelay', 0),
                        properties.get('startTime'),
                        end_time,
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
                        properties['tmc'].get('direction') if properties.get('tmc') else None,
                        current_time,
                        incident_id
                    ))
                    self.conn.commit()
                    return True, False
                else:
                    # Update last_seen even if delay isn't greater
                    cursor.execute('''
                        UPDATE incidents
                        SET last_seen = ?
                        WHERE id = ?
                    ''', (
                        current_time,
                        incident_id
                    ))
                    self.conn.commit()
                    return False, False
            else:
                # Insert new row if incident does not exist
                cursor.execute('''
                    INSERT INTO incidents (
                        id, type, category, geometry_type, coordinates, magnitudeOfDelay, startTime,
                        endTime, from_location, to_location, length, delay, roadNumbers,
                        timeValidity, probabilityOfOccurrence, numberOfReports, lastReportTime,
                        countryCode, tableNumber, tableVersion, direction, last_seen
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    incident_id,
                    incident['type'],
                    properties.get('iconCategory', 0),
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
                    properties['tmc'].get('direction') if properties.get('tmc') else None,
                    current_time
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
        logging.info(f"{inserts} new incident(s) inserted of {changes} changes to DB (of {len(incidents)} current).")
        return changes, inserts

    def mark_ended_incidents(self, threshold_minutes=5):
        """
        Marks incidents as ended if they haven't been seen for threshold_minutes.
        """
        try:
            cursor = self.conn.cursor()
            current_time = datetime.now(UTC)
            threshold_time = current_time - timedelta(minutes=threshold_minutes)
            threshold_iso = threshold_time.isoformat()

            # Select incidents where last_seen is older than threshold and endTime is NULL
            cursor.execute('''
                SELECT id, last_seen, startTime FROM incidents
                WHERE last_seen < ? AND (endTime IS NULL OR endTime = '')
            ''', (threshold_iso, ))

            ended_incidents = cursor.fetchall()

            for (incident_id, last_seen, start_time) in ended_incidents:
                # Calculate startTime + threshold
                start_time_dt = datetime.fromisoformat(start_time)
                start_time_threshold = start_time_dt + timedelta(minutes=threshold_minutes)

                # Determine the latest endTime
                if last_seen is None:
                    end_time = start_time_threshold.isoformat()
                else:
                    last_seen_dt = datetime.fromisoformat(last_seen)
                    end_time = max(last_seen_dt, start_time_threshold).isoformat()

                # Update endTime to the latest value
                cursor.execute('''
                    UPDATE incidents
                    SET endTime = ?
                    WHERE id = ?
                ''', (end_time, incident_id))

            self.conn.commit()
        except Exception as e:
            logging.error(f"Error marking ended incidents: {e}", exc_info=True)

    def export_to_geojson(self, start_datetime, end_datetime, output_file):
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
                SELECT id, type, category, geometry_type, coordinates, magnitudeOfDelay, startTime,
                       endTime, from_location, to_location, length, delay, roadNumbers,
                       timeValidity, probabilityOfOccurrence, numberOfReports, lastReportTime,
                       countryCode, tableNumber, tableVersion, direction
                FROM incidents
                WHERE (startTime BETWEEN ? AND ?) OR (endTime BETWEEN ? AND ?)
            ''', (start_datetime, end_datetime, start_datetime, end_datetime))

            rows = cursor.fetchall()
            features = []
            for row in rows:
                (id_, type_, category, geometry_type, coordinates, magnitudeOfDelay, startTime,
                 endTime, from_location, to_location, length, delay, roadNumbers,
                 timeValidity, probabilityOfOccurrence, numberOfReports, lastReportTime,
                 countryCode, tableNumber, tableVersion, direction) = row

                # Convert coordinates from JSON string to list
                try:
                    coordinates = json.loads(coordinates)
                except json.JSONDecodeError:
                    logging.warning(f"Invalid coordinates for incident ID: {id_}")
                    continue

                # Define geometry
                if geometry_type == 'Point':
                    geometry = geojson.Point(coordinates)
                elif geometry_type == 'LineString':
                    geometry = geojson.LineString(coordinates)
                else:
                    logging.warning(f"Unsupported geometry type for incident ID: {id_}")
                    continue

                # Define properties
                properties = {
                    'id': id_,
                    'type': type_,
                    'category': category,
                    'magnitudeOfDelay': magnitudeOfDelay,
                    'startTime': startTime,
                    'endTime': endTime,
                    'from_location': from_location,
                    'to_location': to_location,
                    'length': length,
                    'delay': delay,
                    'roadNumbers': roadNumbers,
                    'timeValidity': timeValidity,
                    'probabilityOfOccurrence': probabilityOfOccurrence,
                    'numberOfReports': numberOfReports,
                    'lastReportTime': lastReportTime,
                    'countryCode': countryCode,
                    'tableNumber': tableNumber,
                    'tableVersion': tableVersion,
                    'direction': direction
                }

                feature = geojson.Feature(geometry=geometry, properties=properties)
                features.append(feature)

            feature_collection = geojson.FeatureCollection(features)
            with open(output_file, 'w') as f:
                geojson.dump(feature_collection, f)
            logging.info(f"Exported {len(features)} incidents to GeoJSON: {output_file}")
        except Exception as e:
            logging.error(f"Error exporting to GeoJSON: {e}", exc_info=True)

    def get_earliest_and_latest_start_times(self):
        """
        Get the earliest and latest start times from the incidents.
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
                SELECT MIN(startTime), MAX(startTime) FROM incidents
            ''')
            result = cursor.fetchone()
            earliest_start_time, latest_start_time = result
            return earliest_start_time, latest_start_time
        except Exception as e:
            logging.error(f"Error getting earliest and latest start times: {e}", exc_info=True)
            return None, None
        
    def close(self):
        self.conn.close()
