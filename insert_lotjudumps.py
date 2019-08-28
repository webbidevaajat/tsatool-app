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
ch.setFormatter(logging.Formatter('%(asctime)s; %(message)s'))
log.addHandler(ch)
log.setLevel(logging.DEBUG)

class LotjuHandler:
    """
    Parses the contents of LOTJU files of one month
    and inserts them into database.

    Following files MUST be available under project's
    `data/` directory:

    - `anturi_arvo-[year]_[month_with_leading_0].csv`
    - `tiesaa_mittatieto-[year]_[month_with_leading_0].csv`
    - `tiesaa_asema.csv` for station id mapping `ID` -> `VANHA_ID`
    - `laskennallinen_anturi.csv` for sensor id mapping `ID` -> `VANHA_ID`

    :param year:            full year of LOTJU files (int)
    :param month:           month of LOTJU files (int), note that months < 10
                            must appear in the file names as `01`, `02` etc.!
    :param stations_keep:   list of station ids (int) to insert into db,
                            according to Lotju `VANHA_ID`; if empty, all are inserted
    :param sensors_keep:    like above but for sensor ids
    :chunk_size:            max n of rows copied to db at a time
    :limit:                 stop insertions after this amount of rows;
                            if 0, all are inserted
    """
    def __init__(self, year, month,
                 stations_keep=[], sensors_keep=[],
                 chunk_size=1000000, limit=0):
        self.anturi_file = self.check_file("anturi", year, month)
        self.tiesaa_file = self.check_file("tiesaa", year, month)
        self.stations_keep = stations_keep
        self.sensors_keep = sensors_keep
        self.chunk_size = chunk_size
        self.limit = limit
        self.conn = None
        self.warnings = dict()
        self.anturi_mapping = self.convert_ids("laskennallinen_anturi")
        self.tiesaa_mapping = self.convert_ids("tiesaa_asema")
        self.statobs = dict()
        self.n_anturi_inserted = 0
        self.n_tiesaa_inserted = 0

    def check_file(self, srctype, year, month):
        """
        Check that file of `srctype` "anturi" or "tiesaa"
        with given year and month exists under `data/`.
        If yes, return the file path, if not, raise an error.
        """
        if srctype == "anturi":
            candidate = os.path.join("data",
                                     f"anturi_arvo-{year}_{str(month).zfill(2)}.csv")
        elif srctype == "tiesaa":
            candidate = os.path.join("data",
                                     f"tiesaa_mittatieto-{year}_{str(month).zfill(2)}.csv")
        else:
            raise Exception("srctype must be anturi or tiesaa")
        if not os.path.exists(candidate):
            raise FileNotFoundError(candidate)
        return candidate

    def warn_once(self, type, contents, exc_info=False):
        """
        Handle warning logging to a `logger` object
        such that repetitive warnings are not recorded.
        Instead, first ones are logged, and the number of the rest
        can be written in the end of the log.
        """
        if type not in self.warnings.keys():
            self.warnings[type] = 1
            log.warning(f"{type}: {contents} (subsequent warnings of same type are NOT recorded)",
                        exc_info=exc_info)
        else:
            self.warnings[type] += 1

    def log_init_summary(self):
        """
        Log a summary of initial attributes as INFO.
        """
        log.info('START OF LOTJU DUMP INSERTIONS')
        log.info(f'- from {self.tiesaa_file} and {self.anturi_file}')
        if len(self.stations_keep) > 0:
            log.info('- keep stations ' + ', '.join([str(s) for s in self.stations_keep]))
        else:
            log.info('- keep all stations')
        if len(self.sensors_keep) > 0:
            log.info('- keep sensors ' + ', '.join([str(s) for s in self.sensors_keep]))
        else:
            log.info('- keep all sensors')
        log.info(f'- max insert chunk size {self.chunk_size}')
        log.info(f'- row read limit {self.limit}')

    def log_warning_summary(self):
        """
        List number of `warn_once` warnings issued
        as one warning record per type.
        To be called in the end of a script.
        """
        if len(self.warnings) > 0:
            log.warning('Following warnings were calculated:')
        for k, v in self.warnings.items():
            log.warning(f"{v} occurrences of error '{k}'")

    def log_final_summary(self):
        """
        Log a summary in the end of a script.
        """
        log.info('Parsing and inserting routines ended')
        self.log_warning_summary()
        log.info(f'- {self.n_tiesaa_inserted} statobs rows inserted')
        log.info(f'- {self.n_anturi_inserted} seobs rows inserted')
        log.info(f'END OF SCRIPT')

    @staticmethod
    def convert_ids(csv_file, from_field="ID", to_field="VANHA_ID"):
        """
        Read ids from ``csv_file`` located in `data/`,
        return a dict where keys are integer ids from field ``from_field``
        and values are integer ids from field ``to_field``.
        """
        csv_file = os.path.join('data', f'{csv_file}.csv')
        conversion = dict()
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
                    log.error(f'Conversion error on line {i} of file {csv_file}, skipping')
                    continue
        return conversion

    @staticmethod
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

    def parse_tiesaa_mittatieto(self):
        """
        Parse raw ``tiesaa_mittatieto`` file,
        select rows if station id filter provided,
        convert station ids,
        return a dict tree with converted station ids as keys
        and ``obs_id:timestamp`` dictionary as values.
        """
        log.info(f'Parsing {self.tiesaa_file} ...')
        check_statids = len(self.stations_keep) > 0
        with open(self.tiesaa_file, 'r') as fobj:
            reader = csv.DictReader((line.replace('\0','') for line in fobj), delimiter='|')
            i = 0
            for l in reader:
                if i >= self.limit > 0:
                    log.info(f'Stopped tiesaa_mittatieto parsing after row limit {i}')
                    break
                i += 1
                try:
                    lotju_statid = int(l['ASEMA_ID'])
                except ValueError:
                    self.warn_once('Skip tiesaa_mittatieto line because of int ASEMA_ID error',
                                   f'Int conversion of "ASEMA_ID" failed on line {i}, skipping')
                    continue
                try:
                    statid = self.tiesaa_mapping[int(l['ASEMA_ID'])]
                except KeyError:
                    self.warn_once('Skip tiesaa_mittatieto line because of VANHA_ID missing from mapping',
                                   f"Lotju station id {int(l['ASEMA_ID'])} not in tiesaa mapping (line {i})")
                    continue
                if check_statids and statid not in self.stations_keep:
                    continue
                obsid = int(l['ID'])
                ts = self.format_timestamp(l['AIKA'])
                if obsid not in self.statobs.keys():
                    self.statobs[obsid] = {"ts": ts, "statid": statid}
                else:
                    self.warn_once('Skip tiesaa_mittatieto line because of duplicate observation id',
                                   f"Found a duplicate tiesaa_mittatieto id, skipping (line {i})")
        log.info(f'{i} lines parsed from {self.tiesaa_file}, {len(self.statobs)} station observations recorded')

    def insert_statobs_by_keys(self, keys):
        """
        Collect statobs by given keys,
        format for database insertion
        and copy to database table ``statobs``
        (ignoring already existing rows).
        """
        copy_sql = (f"COPY tmp_table (id,tfrom,statid) "
               "FROM STDIN WITH DELIMITER ',';")
        rows = []
        for k, v in self.statobs.items():
            if k not in keys:
                continue
            rows.append([k, v['ts'], v['statid']])
        if len(rows) == 0:
            log.info('No statobs rows to insert')
            return
        rows = '\n'.join([f'{r[0]},{r[1]},{r[2]}' for r in rows])
        with StringIO() as fobj:
            fobj.write(rows)
            fobj.seek(0)
            with self.conn.cursor() as cur:
                try:
                    cur.execute("CREATE TEMP TABLE tmp_table ON COMMIT DROP AS "
                                "SELECT * FROM statobs WITH NO DATA;")
                    cur.copy_expert(sql=copy_sql, file=fobj)
                    cur.execute(f"WITH rows_inserted AS ("
                                "INSERT INTO statobs (SELECT * FROM tmp_table "
                                "ORDER BY tfrom, statid) "
                                "ON CONFLICT DO NOTHING "
                                "RETURNING 1) "
                                "SELECT count(*) FROM rows_inserted;")
                    n_inserted = cur.fetchone()[0]
                    self.conn.commit()
                    log.info(f'{n_inserted} rows inserted into statobs table')
                    self.n_tiesaa_inserted += n_inserted
                except:
                    self.conn.rollback()
                    self.warn_once('Statobs DB insertion error',
                                   'Failed to insert statobs rows',
                                   exc_info=True)

    def insert_seobs(self, rows):
        """
        Format given sensor observation tuples ``rows``
        and insert them into database table ``seobs``
        (ignoring already existing rows).
        """
        copy_sql = (f"COPY tmp_table (id,obsid,seid,seval) "
                    "FROM STDIN WITH DELIMITER ',';")
        if rows is None or len(rows) == 0:
            log.info('No seobs rows to insert')
            return
        rows = '\n'.join([f'{r[0]},{r[1]},{r[2]},{r[3]}' for r in rows])
        with StringIO() as fobj:
            fobj.write(rows)
            fobj.seek(0)
            with self.conn.cursor() as cur:
                try:
                    cur.execute("CREATE TEMP TABLE tmp_table ON COMMIT DROP AS "
                                "SELECT * FROM seobs WITH NO DATA;")
                    cur.copy_expert(sql=copy_sql, file=fobj)
                    cur.execute(f"WITH rows_inserted AS ("
                                "INSERT INTO seobs (SELECT * FROM tmp_table "
                                "ORDER BY obsid, seid) "
                                "ON CONFLICT DO NOTHING "
                                "RETURNING 1) "
                                "SELECT count(*) FROM rows_inserted;")
                    n_inserted = cur.fetchone()[0]
                    self.conn.commit()
                    log.info(f'{n_inserted} rows inserted into seobs table')
                    self.n_anturi_inserted += n_inserted
                except:
                    self.conn.rollback()
                    self.warn_once('Seobs DB insertion error',
                                   'Failed to insert seobs rows',
                                   exc_info=True)

    def parse_and_insert_anturi_arvo(self):
        """
        Parse raw ``anturi_arvo`` file,
        pick rows where ``MITTATIETO_ID`` in ``self.statobs.keys()``,
        possibly filter by (Lotju) sensor id if ``sensors_keep`` given.
        Whenever ``chunk_size`` is achieved, insert the related
        station observations and then the sensor observations into the database.
        Exit if ``limit`` is achieved when parsing the file.
        """
        log.info(f'Parsing {self.anturi_file} ...')
        check_seids = len(self.sensors_keep) > 0
        i = 0
        j = 0
        rows = []
        statobs_keys = set()
        with open(self.anturi_file, 'r') as fobj:
            reader = csv.DictReader((line.replace('\0','') for line in fobj), delimiter='|')
            for l in reader:
                if j == self.chunk_size:
                    self.insert_statobs_by_keys(statobs_keys)
                    statobs_keys = set()
                    self.insert_seobs(rows)
                    rows = []
                    j = 0
                if i >= self.limit > 0:
                    log.info(f'Stopped anturi_arvo parsing after row limit {i}')
                    break
                i += 1
                if i % 10000000 == 0:
                    log.debug(f'{i}th line ...')
                try:
                    mid = int(l['MITTATIETO_ID'])
                except ValueError:
                    self.warn_once('Skip anturi_arvo line because of int MITTATIETO_ID error',
                                   f'Int conversion of MITTATIETEO_ID failed on line {i}, skipping')
                    continue
                if mid not in self.statobs.keys():
                    continue
                try:
                    oid = int(l['ID'])
                except ValueError:
                    self.warn_once('Skip anturi_arvo line because of int ID error',
                                   f'Int conversion of ID failed on line {i}, skipping')
                    continue
                try:
                    aid_lotju = int(l['ANTURI_ID'])
                except ValueError:
                    self.warn_once('Skip anturi_arvo line because of int ANTURI_ID error',
                                   f'Int conversion of ANTURI_ID failed on line {i}, skipping')
                    continue
                try:
                    aid = self.anturi_mapping[aid_lotju]
                except KeyError:
                    self.warn_once('Skip anturi_arvo line because of VANHA_ID missing from mapping',
                                   f'Lotju sensor id {aid_lotju} not in anturi mapping (line {i})')
                    continue
                if check_seids and aid not in self.sensors_keep: # This refers to VANHA_ID!
                    continue
                try:
                    val = float(l['ARVO'])
                except ValueError:
                    self.warn_once('Skip anturi_arvo line because of float ARVO error',
                                   f'Float conversion of ARVO failed on line {i}, skipping')
                    continue
                # id,obsid,seid,seval
                rows.append((oid, mid, aid, val))
                statobs_keys.add(mid)
                j += 1
        if len(statobs_keys) > 0:
            self.insert_statobs_by_keys(statobs_keys)
        if len(rows) > 0:
            self.insert_seobs(rows)

def main():
    parser = argparse.ArgumentParser(description='Convert, filter and insert LOTJU dumps to tsa database.')
    parser.add_argument('year',
                        type=int,
                        help='Dump file year identifier')
    parser.add_argument('month',
                        type=int,
                        help='Dump file month identifier')
    parser.add_argument('--stations',
                        type=int,
                        help='Station ids to insert data from, sep by space, or empty for all available',
                        default=[],
                        nargs='+')
    parser.add_argument('--sensors',
                        type=int,
                        help='Sensor ids to insert data from, sep by space, or empty for all available',
                        default=[],
                        nargs='+')
    parser.add_argument('--chunk',
                        type=int,
                        help='Max number of rows to insert into database at a time',
                        default=1000000)
    parser.add_argument('--limit',
                        type=int,
                        help='Max number of lines to read from a dump file, or empty/non-positive to read all',
                        default=0)
    args = parser.parse_args()
    conn = None
    try:
        lotju_hdl = LotjuHandler(year=args.year,
                                 month=args.month,
                                 stations_keep=args.stations,
                                 sensors_keep=args.sensors,
                                 chunk_size=args.chunk,
                                 limit=args.limit)
        conn = tsadb_connect()
        lotju_hdl.conn = conn
        lotju_hdl.log_init_summary()
        lotju_hdl.parse_tiesaa_mittatieto()
        lotju_hdl.parse_and_insert_anturi_arvo()
        lotju_hdl.log_final_summary()
    except Exception as e:
        log.exception('Script interrupted')
    finally:
        if conn is not None:
            conn.close()
        for h in log.handlers:
            h.close()

if __name__ == '__main__':
    main()
