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
# fh = logging.FileHandler(os.path.join('logs', 'lotjudumps.log'))
# fh.setFormatter(formatter)
# log.addHandler(fh)
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(logging.Formatter('%(asctime)s; %(message)s'))
log.addHandler(ch)
log.setLevel(logging.DEBUG)

def convert_ids(csv_file, from_field, to_field):
    """
    Read ids from ``csv_file``,
    return a dict where keys are integer ids from field ``from_field``
    and values are integer ids from field ``to_field``.
    """
    conversion = {}
    with open(csv_file, 'r', newline='') as fobj:
        reader = csv.DictReader(fobj, delimiter=',', quotechar='"')
        i = 0
        for l in reader:
            i += 1
            try:
                from_id = int(l[from_field])
                to_id = int(l[to_field])
                conversion[from_id] = to_id
            except ValueError:
                log.warning(f'Conversion error on line {i} of file {csv_file}, skipping')
                continue
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
    select rows if station id selection provided,
    convert station ids,
    return a dict tree with converted station ids as keys
    and ``obs_id:timestamp`` dictionary as values.

    NOTE: ``station_ids`` filter must correspond to the "from_id" (short id),
    i.e., they must be converted first!
    """
    statobs = {}
    check_statids = len(station_ids) > 0
    with open(csv_file, 'r') as fobj:
        reader = csv.DictReader(fobj, delimiter='|')
        i = 0
        for l in reader:
            i += 1
            try:
                short_statid = int(l['ASEMA_ID'])
            except ValueError:
                log.warning(f'Int conversion failed on line {i}, skipping')
                continue
            if check_statids:
                if short_statid not in station_ids:
                    continue
            try:
                statid = conversion[int(l['ASEMA_ID'])]
            except KeyError:
                log.warning((f"Statid {int(l['ASEMA_ID'])} not in conversion (line {i})"))
                continue
            obsid = int(l['ID'])
            ts = format_timestamp(l['AIKA'])
            if statid not in statobs.keys():
                statobs[statid] = {obsid: ts}
            else:
                statobs[statid][obsid] = ts
    return statobs

def flatten_tiesaa_mittatieto(parsed_tree):
    """
    Flatten tiesaa_mittatieto data from dict tree
    to database-ready rows.
    """
    rows = []
    for k, v in parsed_tree.items():
        stid = k
        for mid, ts in v.items():
            rows.append(f'{mid},{ts},{stid}')
    return rows

def parse_anturi_arvo(csv_file, tsm_ids, seid_conv, row_limit):
    """
    Parse raw ``anturi_arvo`` file,
    filter rows by '``MITTATIETO_ID`` in ``tsm_ids``,
    convert ``ANTURI_ID``,
    return database-ready rows as list.
    """
    rows = []
    with open(csv_file, 'r') as fobj:
        reader = csv.DictReader(fobj, delimiter='|')
        i = 0
        for l in reader:
            i += 1
            if row_limit > 0 and i > row_limit:
                return rows
            if i % 1000000 == 0:
                log.debug(f'Line {i} ...')
            try:
                mid = int(l['MITTATIETO_ID'])
            except ValueError:
                log.warning(f'Int conversion of MITTATIETO_ID failed on line {i}, skipping')
            if mid not in tsm_ids:
                continue
            try:
                oid = int(l['ID'])
            except ValueError:
                log.warning(f'Int conversion of ID failed on line {i}, skipping')
                continue
            try:
                aid = seid_conv[int(l['ANTURI_ID'])]
            except ValueError:
                log.warning(f'Int conversion of ANTURI_ID failed on line {i}, skipping')
                continue
            except KeyError:
                log.warning(f'Conversion not found for ANTURI_ID on line {i}, skipping')
                continue
            try:
                val = float(l['ARVO'])
            except ValueError:
                log.warning(f'Float conversion of ARVO failed on line {i}, skipping')
                continue
            rows.append(f'{oid},{mid},{aid},{val}')
    return rows


def copy_to_table(conn, fields, rows, table, chunk_size=1000000):
    """
    Copy ``rows`` to comma-separated ``fields``
    of database ``table`` over ``conn``
    in chunks of ``chunk_size`` rows.

    NOTE: We first copy data to temp table and then insert it
    with the upsert functionality (ON CONFLICT DO NOTHING), since
    ignoring duplicate primary key rows is not directly possible with COPY FROM.
    """
    copy_sql = (f"COPY tmp_table ({fields}) "
           "FROM STDIN WITH DELIMITER ',';")
    nrows = len(rows)
    log.info(f'Copying {nrows} rows to table {table} in chunks of {chunk_size}')
    try:
        i = 0
        j = min(chunk_size, nrows)
        while j <= min(chunk_size, nrows) and i < j:
            log.info(f'{i}/{nrows} ...')
            debug_rows = '\n'.join(rows[i:(i+3)]) + '\n...\n' + '\n'.join(rows[(j-3):j])
            log.debug('\n' + debug_rows)
            with StringIO() as fobj:
                fobj.write('\n'.join(rows[i:j]))
                fobj.seek(0)
                with conn.cursor() as cur:
                    cur.execute("CREATE TEMP TABLE tmp_table ON COMMIT DROP AS "
                                f"SELECT * FROM {table} WITH NO DATA;")
                    cur.copy_expert(sql=copy_sql, file=fobj)
                    cur.execute(f"INSERT INTO {table} SELECT * FROM tmp_table "
                                "ON CONFLICT DO NOTHING;")
                    conn.commit()
            i = j
            j = min(j + chunk_size, nrows)
        log.info(f'All rows copied to table {table}')
    except:
        conn.rollback()
        log.exception('Error copying to db, rolling back')

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
                        help='Max number of anturi_arvo lines to read, or empty/non-positive to read all',
                        default=0)
    parser.add_argument('-c', '--conversions',
                        type=str,
                        help='ID conversion files 1) laskennallinen_anturi and 2) tiesaa_asema',
                        default=[os.path.join('data', 'tiesaa_asema.csv'),
                                 os.path.join('data', 'laskennallinen_anturi.csv')],
                        nargs=2)
    parser.add_argument('-u', '--username',
                        type=str,
                        help='Database username',
                        default='postgres')
    parser.add_argument('-p', '--password',
                        type=str,
                        help='Database password',
                        default='postgres')
    args = parser.parse_args()

    log.info('STARTING LOTJUDUMPS INSERTION')
    conn = None
    try:
        conn = tsadb_connect(username=args.username, password=args.password)

        # ID CONVERSIONS
        statid_conv = convert_ids(csv_file=args.conversions[0],
                                from_field='ID',
                                to_field='VANHA_ID')
        seid_conv = convert_ids(csv_file=args.conversions[1],
                                from_field='ID',
                                to_field='VANHA_ID')
        log.info(f'{len(statid_conv)} station ids and {len(seid_conv)} sensor ids converted')

        # TIESAA_MITTATIETO
        # We use inverted station id conversion here
        # so we can compare "short" station ids in raw data directly.
        statid_conv_inv = {v:k for k, v in statid_conv.items()}
        statid_short_selected = []
        for sid in args.stations:
            try:
                statid_short_selected.append(statid_conv_inv[sid])
            except KeyError:
                log.warning(f'Statid {sid} omitted: no corresponding short id found')
        tsm_parsed = parse_tiesaa_mittatieto(csv_file=args.tiesaa_mittatieto,
                                             conversion=statid_conv,
                                             station_ids=statid_short_selected)
        tsm_report_els = [(k, len(v)) for k, v in tsm_parsed.items()]
        tsm_report_els = sorted(tsm_report_els, key=lambda el: el[0])
        tsm_report_els = '\n'.join([f'{k}:\t{v}' for k, v in tsm_report_els])
        log.info((f'Number of tiesaa_mittatieto elements parsed per station:\n'
                  f'{tsm_report_els}'))

        # ANTURI_ARVO
        # tiesaa_mittatieto observation ids are combined
        # so they can be used to filter only required anturi_arvo
        # observations already when parsing the large raw data file.
        # TODO: also filter sensor ids?
        tsm_obsids = set()
        for v in tsm_parsed.values():   # Dictionaries under station ids
            for k in v.keys():          # Obs ids as keys
                tsm_obsids.add(k)
        aa_parsed = parse_anturi_arvo(csv_file=args.anturi_arvo,
                                      tsm_ids=tsm_obsids,
                                      seid_conv=seid_conv,
                                      row_limit=args.limit)
        log.info(f'{len(aa_parsed)} anturi_arvo rows')

        # DATABASE INSERTIONS
        # tiesaa_mittatieto data are flattened first
        tsm_flat = flatten_tiesaa_mittatieto(tsm_parsed)
        copy_to_table(conn,
                      fields='id,tfrom,statid',
                      rows=tsm_flat,
                      table='statobs')
        copy_to_table(conn,
                      fields='id,obsid,seid,seval',
                      rows=aa_parsed,
                      table='seobs')

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
