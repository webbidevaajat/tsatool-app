#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This script filters given 'anturi' and 'tiesaa' csv datasets
(which should be of LOTJU format)
by given station ids and copies them
to corresponding 'seobs' and 'statobs' data tables in the tsa database,
converting sensor and station ids between new and old id systems.

Example LOTJU ``anturi_arvo-YYYY-MM.csv`` contents:
```
ID|ANTURI_ID|ARVO|MITTATIETO_ID
23453494515|1|0.5|411814242
23453494576|7|-0.3|411814242
23453494577|9|-3.1|411814242
```

Example LOTJU ``tiesaa_mittatieto-YYYY-MM.csv`` contents:
```
ID|AIKA|ASEMA_ID
411813852|01.01.2018 00:00:00,000000000|202
411813860|01.01.2018 00:00:00,000000000|223
411813867|01.01.2018 00:00:00,000000000|229
```
"""

import logging
import os
import sys
import csv
import pytz
import argparse
import psycopg2
from io import StringIO
from tsa import tsadb_connect
from datetime import datetime

log = logging.getLogger(__name__)
formatter = logging.Formatter('%(asctime)s; %(levelname)s; %(message)s',
                              '%Y-%m-%d %H:%M:%S')
fh = logging.FileHandler(os.path.join('logs', 'lotjudumps.log'))
fh.setFormatter(formatter)
log.addHandler(fh)
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(logging.Formatter('%(message)s'))
log.addHandler(ch)
log.setLevel(logging.DEBUG)

def copy_to_table(conn, data, table):
    """
    Copy StringIO ``data`` to database ``table`` over ``conn``
    """
    try:
        data.seek(0)
        with conn.cursor() as cur:
            # TODO: use copy_expert
            cur.copy_from(
                file=data,
                table=table
            )
        conn.commit()
        log.info('copied to {}'.format(table))
    except psycopg2.InternalError as e:
        conn.rollback()
        log.exception('DB internal error, rolling back')
    except Exception as e:
        log.exception('could not copy to table')

def format_tiesaa_mittatieto_line(l):
    """
    Prepare line ``l`` for ``tiesaa_mittatieto`` insertion
    """
    try:
        l = l.strip().split('|')
        assert len(l) == 3
        if 'ID' in l[0]:
            log.info('header line detected, skipping')
            return None
        # Format timestamp
        l[1] = l[1].split(',')[0]
        l[1] = datetime.strptime(l[1], '%d.%m.%Y %H:%M:%S')
        l[1] = l[1].strftime('%Y-%m-%d %H:%M:%S')
        return f"{l[0]}\t'{l[1]}'\t{l[2]}\n"
    except Exception as e:
        log.exception('could not format line')
        return None

def format_anturi_arvo_line(l):
    """
    Prepare line ``l`` for ``anturi_arvo`` insertion
    """
    try:
        l = l.strip().split('|')
        assert len(l) == 5
        return '\t'.join(l[:4]) + '\n'
    except Exception as e:
        log.exception('could not format line')
        return None

def convert_ids(csv_file, from_field, to_field):
    """
    Read ids from ``csv_file``,
    return a dict where keys are integer ids from field ``from_field``
    and values are integer ids from field ``to_field``.
    """
    conversion = {}
    with open(csv_file, 'r') as fobj:
        reader = csv.DictReader(fobj, delimiter=',', quotechar='"')
        for l in reader:
            from_id = int(l[from_field])
            to_id = int(l[to_field])
            conversion[from_id] = to_id
    return conversion

def format_timestamp(ts_str):
    """
    Parse ``dd.mm.YYYY HH:MM:SS,0000...`` timestamp string
    (used in Lotju files) into datetime,
    assume timezone 'Europe/Helsinki',
    and convert into standard UTC timestamp string.
    """
    ts = ts_str.split(',')[0]
    ts = datetime.strptime(ts, '%d.%m.%Y %H:%M:%S')
    ts = pytz.timezone('Europe/Helsinki').localize(ts)
    ts = ts.astimezone(pytz.timezone('UTC'))
    ts = ts.strftime('%Y-%m-%d %H:%M:%S%z')
    return ts

def parse_tiesaa_mittatieto(csv_file, conversion, station_ids):
    """
    Parse raw ``tiesaa_mittatieto`` file,
    convert station ids, select rows if station id selection provided,
    return a dict tree with converted station ids as keys
    and ``obs_id:timestamp`` dictionary lists as values.
    """
    statobs = {}
    check_statids = len(station_ids) > 0
    with open(csv_file, 'r') as fobj:
        reader = csv.DictReader(fobj, delimiter='|')
        for l in reader:
            obsid = int(l['ID'])
            ts = format_timestamp(l['AIKA'])
            statid = conversion[int(l['ASEMA_ID'])]
            if check_statids:
                if statid not in station_ids:
                    continue
            # TODO: add station id to statobs keys,
            # init obsid:ts list,
            # add entries to that list

def main():
    parser = argparse.ArgumentParser(description='Insert LOTJU dumps to tsa database.')
    parser.add_argument('-a', '--anturi_arvo',
                        type=str,
                        help='Path or URL of anturi_arvo file',
                        metavar='ANTURI_ARVO_FILE',
                        required=True)
    parser.add_argument('-t', '--tiesaa_mittatieto',
                        type=str,
                        help='Path or URL of tiesaa_mittatieto file',
                        metavar='TIESAA_MITTATIETO_FILE',
                        required=True)
    parser.add_argument('-s', '--stations',
                        type=int,
                        help='Station ids to insert data from, sep by space, or empty for all available',
                        default=[],
                        nargs='+')
    parser.add_argument('-l', '--limit',
                        type=int,
                        help='Limit number of station observations to insert, or empty for all available',
                        default=0)
    parser.add_argument('-c', '--conversions',
                        type=str,
                        help='ID conversion files 1) laskennallinen_anturi and 2) tiesaa_asema'
                        default=[os.path.join('data', 'laskennallinen_anturi.csv'),
                                 os.path.join('data', 'tiesaa_asema.csv')],
                        nargs=2)
    parser.add_argument('-u', '--username',
                        type=str,
                        help='Database username',
                        default='postgres')
    parser.add_argument('-p', '--password',
                        type='str',
                        help='Database password',
                        default='')
    args = parser.parse_args()

    log.info('STARTING LOTJUDUMPS INSERTION')
    conn = None
    try:
        conn = tsadb_connect(username=args.username, password=args.password)

        # ID CONVERSIONS
        stid_conv = convert_ids(csv_file=args.conversions[0],
                                from_field='ID',
                                to_field='VANHA_ID')
        seid_conv = convert_ids(csv_file=args.conversions[1],
                                from_field='ID',
                                to_field='VANHA_ID')
        log.info(f'{len(stid_conv)} station ids and {len(seid_conv)} sensor ids converted')

        # TIESAA_MITTATIETO INSERTIONS
        sel_ids = args.stations
        filter_ids = len(sel_ids) > 0


        # ANTURI_ARVO INSERTIONS


        log.info('END OF SCRIPT')
    except Exception as e:
        log.exception('Script interrupted')
    finally:
        if conn:
            conn.close()
        for h in log.handlers:
            h.close()

if __name__ == '__main__':
    main()
