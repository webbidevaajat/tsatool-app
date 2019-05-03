#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Save station and sensor metadata from
lotju dump to csv with id, short name and old id,
in order to check for old-new id correspondence
in sensors and stations.

After running the script,
review the csv files manually and copy to database.
"""

import os
import psycopg2
import urllib.request
# from io import StringIO
from tsa import tsadb_connect

def main():
    # Stations
    u = 'https://tiesaahistoria-jakelu.s3.amazonaws.com/2018/tiesaa_asema.csv'
    with urllib.request.urlopen(u, timeout=60) as urlconn:
        # output = StringIO()
        output = open(os.path.join('data', 'tiesaa_asema.csv'), 'w')
        i = 0
        while True:
            i += 1
            l = urlconn.readline().decode('utf-8')
            if not l:
                break
            try:
                l = l.split('|')
                l = f'{l[0]},{l[3]},{l[35]}\n'
                output.write(l)
            except:
                print(f'Could not write line {i}')
        output.close()
        print('tiesaa_asema written')

    # Sensors
    u = 'https://tiesaahistoria-jakelu.s3.amazonaws.com/2018/laskennallinen_anturi.csv'
    with urllib.request.urlopen(u, timeout=60) as urlconn:
        # output = StringIO()
        output = open(os.path.join('data', 'laskennallinen_anturi.csv'), 'w')
        i = 0
        while True:
            i += 1
            l = urlconn.readline().decode('utf-8')
            if not l:
                break
            try:
                l = l.split('|')
                l = f'{l[0]},{l[5]},{l[9]}\n'
                output.write(l)
            except:
                print(f'Could not write line {i}')
        output.close()
        print('laskennallinen_anturi written')

if __name__ == '__main__':
    main()
