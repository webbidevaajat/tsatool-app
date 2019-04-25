#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Iterate through tiesaa_mittatieto/*_subset.csv files,
convert old station ids to new ones and insert to db table "statobs".
Iterate through anturi_arvo/*_subset.csv files,
convert old sensor ids to new ones and insert to db table "seobs".
"""

import os
import csv
import sys
import logging
import psycopg2
from io import StringIO
from tsa import tsadb_connect

N_FIRST = 1
N_LAST = 12

log = logging.getLogger(__name__)
formatter = logging.Formatter('%(asctime)s; %(levelname)s; %(message)s',
                              '%Y-%m-%d %H:%M:%S')
fh = logging.FileHandler(os.path.join('logs', 'statobs_seobs_insertion.log'))
fh.setFormatter(formatter)
log.addHandler(fh)
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(logging.Formatter('%(message)s'))
log.addHandler(ch)
log.setLevel(logging.DEBUG)

def pairs(csvfile, key='id', value='vanha_id'):
    """
    Read csv file containing "id" and "vanha_id" fields.
    Return dictionary where key="id" and value="vanha_id".
    This applies to both tiesaa_mittatieto and anturi_arvo files.
    """
    d = {}
    i = 0
    with open(csvfile, 'r') as fobj:
        rdr = csv.DictReader(fobj, delimiter=',', quotechar='"')
        for l in rdr:
            i += 1
            d[l[key]] = l[value]
    log.info(f'{i} id pairs read from {csvfile}')
    return d

def parse_statobs_subset(n, pairs):
    infilename = os.path.join('data',
                              'tiesaa_mittatieto',
                              f'tiesaa_mittatieto-2018_{n:02}_subset.csv')
    i = 0
    out = StringIO()
    writer = csv.DictWriter(out,
                            fieldnames=['id', 'tfrom', 'statid'],
                            delimiter=',')
    writer.writeheader()

    with open(infilename, 'r') as infile:
        reader = csv.DictReader(infile, delimiter='|')
        for l in reader:
            d = {'id': l['ID'],
                 'tfrom': l['AIKA'].split(',')[0],
                 'statid': pairs[l['ASEMA_ID'].strip()]}
            writer.writerow(d)
            i += 1

    log.info(f'{i} lines converted from {infilename}')
    return out

def parse_seobs_subset(n, pairs):
    infilename = os.path.join('data',
                              'anturi_arvo',
                              f'anturi_arvo-2018_{n:02}_subset.csv')
    i = 0
    out = StringIO()
    writer = csv.DictWriter(out,
                            fieldnames=['id', 'obsid', 'seid', 'seval'],
                            delimiter=',')
    writer.writeheader()

    with open(infilename, 'r') as infile:
        reader = csv.DictReader(infile, delimiter='|')
        for l in reader:
            d = {'id': l['ID'],
                 'obsid': l['MITTATIETO_ID'],
                 'seid': pairs[l['ANTURI_ID'].strip()],
                 'seval': l['ARVO']}
            writer.writerow(d)
            i += 1

    log.info(f'{i} lines converted from {infilename}')
    return out

def main():
    station_pairs = pairs(os.path.join('data', 'station_pairs.csv'))
    sensor_pairs = pairs(os.path.join('data', 'sensor_pairs.csv'))

    # Connect to db
    conn = tsadb_connect()

    # Set datetime format
    with conn.cursor() as cur:
        try:
            cur.execute("SET datestyle = 'ISO,DMY';")
            conn.commit()
        except:
            log.error(sys.exc_info()[0])
            conn.rollback()

    # tiesaa_mittatieto subset -> convert -> statobs
    for n in range(N_FIRST, N_LAST+1):
        log.info(f'tiesaa_mittatieto number {n}')
        csv_contents = parse_statobs_subset(n=n, pairs=station_pairs)
        csv_contents.seek(0)
        sql = ("COPY statobs (id, tfrom, statid)"
               "FROM STDIN WITH DELIMITER ',' CSV HEADER;")
        with conn.cursor() as cur:
            try:
                cur.copy_expert(sql=sql, file=csv_contents)
                conn.commit()
            except:
                log.error(sys.exc_info()[0])
                conn.rollback()
            finally:
                csv_contents.close()

    # anturi_arvo subset -> convert -> seobs
    for n in range(N_FIRST, N_LAST+1):
        log.info(f'anturi_arvo number {n}')
        csv_contents = parse_seobs_subset(n=n, pairs=sensor_pairs)
        csv_contents.seek(0)
        sql = ("COPY seobs (id, obsid, seid, seval)"
               "FROM STDIN WITH DELIMITER ',' CSV HEADER;")
        with conn.cursor() as cur:
            try:
                cur.copy_expert(sql=sql, file=csv_contents)
                conn.commit()
            except:
                log.error(sys.exc_info()[0])
                conn.rollback()
            finally:
                csv_contents.close()

    log.info('END OF SCRIPT')

if __name__ == '__main__':
    main()
