#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Iterate through monthly tiesaa_mittatieto files and save
versions of them with listed station ids only.
Then, iterate through monthly anturi_arvo files and save
versions of them with observation ids existing in the filtered
tiesaa_mittatieto files only.
These files can then be inserted to the database raw data tables.
"""

import os
import csv
import sys
import logging

log = logging.getLogger(__name__)
formatter = logging.Formatter('%(asctime)s; %(levelname)s; %(message)s',
                              '%Y-%m-%d %H:%M:%S')
fh = logging.FileHandler(os.path.join('logs', 'filter_raw_data.log'))
fh.setFormatter(formatter)
log.addHandler(fh)
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(logging.Formatter('%(message)s'))
log.addHandler(ch)
log.setLevel(logging.DEBUG)

STATIDS = [
1006, 1011, 1019, 1022, 1083, 1101, 1103, 1104, 1105, 1106, 1107,
1108, 1109, 1110, 1111, 1115, 1116, 1118, 1119, 1120, 1121, 1122,
1123, 1124, 1125, 1126, 1131, 1132, 3001, 3002, 3006, 3023, 3024,
3029, 3030, 3045, 3047, 3048, 3051, 3052, 3053, 3054, 3056, 3057,
3058, 3059, 3062, 3063, 3064, 3065, 3066, 3067, 3074, 3075, 3077,
3078, 3079, 3080, 3081, 3082
]

def pairs(csvfile):
    """
    Read comma separated `a, b` integer pairs
    and return a dictionary of form `b: a`
    """
    ls = []
    i = 0
    with open(csvfile, 'r') as fobj:
        for l in fobj:
            i += 1
            pair = l.strip().split(',')
            if len(pair) < 2:
                log.info(f'{i} pairs read')
                break
            try:
                ls.append((int(pair[1]), int(pair[0])))
            except ValueError:
                log.info(f'Passed line {i}')
                continue
    return dict(ls)

def sort_and_filter_mt(n, station_ids):
    mt_dir = os.path.join(
        os.getcwd(),
        'data', 'tiesaa_mittatieto')
    mt_inpath = os.path.join(mt_dir,
        f'tiesaa_mittatieto-2018_{n:02}.csv')
    mt_outpath = os.path.join(mt_dir,
        f'tiesaa_mittatieto-2018_{n:02}_subset.csv')
    observation_ids = set()
    ls = []
    nrows = 0
    with open(mt_inpath, 'r') as mt_infile:
        rdr = csv.DictReader(mt_infile, delimiter='|')
        fields = rdr.fieldnames
        for l in rdr:
            sid = int(l['ASEMA_ID'])
            if sid in station_ids:
                observation_ids.add(int(l['ID']))
                ls.append(l)
                nrows += 1
        ls = sorted(ls, key=lambda x: int(x['ID']))
        with open(mt_outpath, 'w', newline='') as mt_outfile:
            wrtr = csv.DictWriter(
                mt_outfile,
                fieldnames=fields,
                delimiter='|'
            )
            wrtr.writeheader()
            for l in ls:
                wrtr.writerow(l)
    log.info(f'{nrows} lines saved to file {mt_outpath}')
    return observation_ids

def filter_aa(n, observation_ids):
    aa_dir = os.path.join(
        os.getcwd(),
        'data', 'anturi_arvo')
    aa_inpath = os.path.join(aa_dir,
        f'anturi_arvo-2018_{n:02}.csv')
    aa_outpath = os.path.join(aa_dir,
        f'anturi_arvo-2018_{n:02}_subset.csv')
    keys_out = ["ID", "ANTURI_ID", "ARVO", "MITTATIETO_ID"]
    i = 0
    j = 0
    with open(aa_inpath, 'r') as aa_infile:
        rdr = csv.DictReader(aa_infile, delimiter='|')
        # NOTE: last field "TIEDOSTO_ID" is left out
        assert keys_out == rdr.fieldnames[:-1]
        with open(aa_outpath, 'w', newline='') as aa_outfile:
            wrtr = csv.DictWriter(
                aa_outfile,
                fieldnames=keys_out,
                delimiter='|'
            )
            wrtr.writeheader()
            for l in rdr:
                el = {k:v for k, v in l.items() if k in keys_out}
                i += 1
                if int(el['MITTATIETO_ID']) in observation_ids:
                    wrtr.writerow(el)
                    j += 1
                if i % 100000 == 0:
                    log.info(f'{i} lines read')
    log.info(f'{j} of {i} lines saved to files {aa_outpath}')


def main():
    log.info('Using following station ids:')
    log.info(STATIDS)
    st_pairs = pairs(os.path.join(os.getcwd(), 'data', 'st_pairs.txt'))
    statids_conv = [st_pairs[k] for k in STATIDS]
    log.info(statids_conv)
    if input('Continue? ') != 'y':
        sys.exit()
    for n in range(1, 13):
        log.info(f'Number {n}')
        try:
            mt_ids = sort_and_filter_mt(n, statids_conv)
            filter_aa(n, mt_ids)
        except:
            continue

if __name__ == '__main__':
    main()
