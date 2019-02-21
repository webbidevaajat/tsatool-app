#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This script fetches TSA station and sensor data
from Digitraffic API, using the current dataset available,
and saves it to the respective tsa database tables.
For now, possible old data is deleted from the tables
and replaced by the new dataset.

Note that ``db_config.json`` file must be available
to connect to the database. Database admin credentials
are needed for the insertion operations.
"""

import psycopg2 as pg
import requests
import json
import sys
from getpass import getpass
from psycopg2.extras import execute_values

from tsa import tsadb_connect

def get_stations():
    """
    Fetches station data from Digitraffic API
    and prepares it for database insertion.

    :return: rows to be inserted into ``stations`` table
    :rtype: tuple
    """
    endpoint = 'http://tie.digitraffic.fi/api/v1/metadata/weather-stations'
    resp = requests.get(endpoint)
    if resp.status_code != 200:
        print('Could not fetch station results:')
        print('Status code {:d}'.format(resp.status_code))
        return None

    # Assuming that the response has exactly the correct JSON structure;
    # otherwise a KeyError is raised, probably.
    feats = resp.json()['features']
    print('{:d} stations fetched from Digitraffic API.'.format(len(feats)))

    # We do not use the exact same data structure for saving to the db
    # so the required fields are picked by hand into a list of tuples
    db_rows = []
    for f in feats:
        id = f['id']
        geom = json.dumps(f['geometry'], ensure_ascii=False)
        prop = json.dumps(f['properties'], ensure_ascii=False)
        db_rows.append((id, geom, prop))

    return db_rows

def get_sensors():
    """
    Fetches sensor data from Digitraffic API
    and prepares it for database insertion.

    :return: rows to be inserted into ``sensors`` table
    :rtype: tuple
    """
    endpoint = 'http://tie.digitraffic.fi/api/v1/metadata/weather-sensors'
    resp = requests.get(endpoint)
    if resp.status_code != 200:
        print('Could not fetch sensor results:')
        print('Status code {:d}'.format(resp.status_code))
        return None

    feats = resp.json()['roadStationSensors']
    print('{:d} sensors fetched from Digitraffic API.'.format(len(feats)))

    db_rows = []
    for f in feats:
        id = f['id']
        name = f['name']
        shortname = f['shortName']
        unit = f['unit']
        accuracy = f['accuracy']
        nameold = f['nameOld']
        valuedescriptions = json.dumps(f['sensorValueDescriptions'],
                                       ensure_ascii=False)
        description = f['descriptions']['fi']
        db_rows.append((id,
                        name,
                        shortname,
                        unit,
                        accuracy,
                        nameold,
                        valuedescriptions,
                        description))

    return db_rows

def main():
    print('\nFetch and insert')
    print('STATIONS and SENSORS')
    print('from Digitraffic into TSA database')
    print('\n******************************\n')
    pg_conn = tsadb_connect(ask=True)
    if not pg_conn:
        sys.exit()

    print('Connected to database.')
    cur = pg_conn.cursor()

    # **************************
    # Operations on STATION data
    write = False
    stations = get_stations()
    if input('Do you want to write current '
             'station data to database? [y if yes] ') == 'y':
        write = True
    else:
        print('Skipped inserting station data.')
    cur.execute("SELECT COUNT(*) FROM stations;")
    exist_count = cur.fetchone()[0]
    if exist_count > 0:
        print('Table "stations" already has {:d} rows.'.format(exist_count))
        if input('Do you want to overwrite old rows? [y if yes] ') != 'y':
            print('Skipped inserting station data.')
            write = False

    if write:
        if exist_count > 0:
            cur.execute("DELETE FROM stations;")
            pg_conn.commit()
            print('{:d} rows deleted from table "stations".')
        insert_query = ("INSERT INTO stations (id, geom, prop) "
                        "VALUES %s")
        psycopg2.extras.execute_values(cur=cur,
                                       sql=insert_query,
                                       argslist=stations)
        pg_conn.commit()
        print('{:d} rows inserted into "stations".'.format(len(stations)))

    # **************************

    # Operations on SENSOR data
    # *************************
    write = False
    sensors = get_sensors()
    if input('Do you want to write current '
             'station data to database? [y if yes] ') == 'y':
        write = True
    else:
        print('Skipped inserting sensor data.')
    cur.execute("SELECT COUNT(*) FROM sensors;")
    exist_count = cur.fetchone()[0]
    if exist_count > 0:
        print('Table "sensors" already has {:d} rows.'.format(exist_count))
        if input('Do you want to overwrite old rows? [y if yes] ') != 'y':
            print('Skipped inserting sensor data.')
            write = False

    if write:
        if exist_count > 0:
            cur.execute("DELETE FROM sensors;")
            pg_conn.commit()
            print('{:d} rows deleted from table "sensors".')
        insert_query = ("INSERT INTO sensors "
                        "(id, name, shortname, unit, "
                        "accuracy, nameold, valuedescriptions, description) "
                        "VALUES %s")
        psycopg2.extras.execute_values(cur=cur,
                                       sql=insert_query,
                                       argslist=sensors)
        pg_conn.commit()
        print('{:d} rows inserted into "sensors".'.format(len(sensors)))

    # *************************

    cur.close()
    pg_conn.close()
    print('END OF SCRIPT')

if __name__ == '__main__':
    main()
