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
import psycopg2
import urllib.request
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

def main():
    log.info('START OF A NEW SESSION')
    interactive = False
    cont = 'y'
    conn = None
    # How many rows are inserted at a time:
    chunk_limit = 100000
    try:
        conn = tsadb_connect()
        months = [f'{s:02d}' for s in range(1, 3)]
        base_url = 'https://tiesaahistoria-jakelu.s3.amazonaws.com/2018/'
        tiesaa_urls = [f'{base_url}tiesaa_mittatieto-2018_{m}.csv' for m in months]
        log.info('using following tiesaa urls:\n{}'.format('\n'.join(tiesaa_urls)))
        anturi_urls = [f'{base_url}anturi_arvo-2018_{m}.csv' for m in months]
        log.info('using following anturi urls:\n{}'.format('\n'.join(anturi_urls)))

        # TIESAA_MITTATIETO INSERTIONS
        log.info('starting tiesaa_mittatieto insertions')
        for u in tiesaa_urls:
            log.info(f'Going to copy from {u}')
            if interactive:
                cont = input('Continue? [y if yes] ')
            if cont != 'y':
                raise Exception('no user permission to continue')
            i = 0
            i_tot = 0
            try:
                with urllib.request.urlopen(u, timeout=60) as urlconn:
                    output = StringIO()
                    while True:
                        l = urlconn.readline().decode('utf-8')
                        if not l:
                            if output:
                                copy_to_table(conn, output, 'tiesaa_mittatieto')
                            output.close()
                            break
                        if i >= chunk_limit:
                            copy_to_table(conn, output, 'tiesaa_mittatieto')
                            output.close()
                            output = StringIO()
                            i_tot += i
                            log.info(f'{i_tot} lines written so far')
                            i = 0
                        l = format_tiesaa_mittatieto_line(l)
                        if l:
                            output.write(l)
                        i += 1
                log.info(f'{i_tot} lines from {u} written to tiesaa_mittatieto')
            except Exception as e:
                log.exception('write operation failed')

        # ANTURI_ARVO INSERTIONS
        log.info('starting anturi_arvo insertions')
        for u in anturi_urls:
            log.info(f'Going to copy from {u}')
            if interactive:
                cont = input('Continue? [y if yes] ')
            if cont != 'y':
                raise Exception('no user permission to continue')
            i = 0
            i_tot = 0
            try:
                with urllib.request.urlopen(u, timeout=60) as urlconn:
                    output = StringIO()
                    while True:
                        l = urlconn.readline().decode('utf-8')
                        if not l:
                            if output:
                                copy_to_table(conn, output, 'anturi_arvo')
                            output.close()
                            break
                        if i >= chunk_limit:
                            copy_to_table(conn, output, 'anturi_arvo')
                            output.close()
                            output = StringIO()
                            i_tot += i
                            log.info(f'{i_tot} lines written so far')
                            i = 0
                        l = format_anturi_arvo_line(l)
                        if l:
                            output.write(l)
                        i += 1
                log.info(f'{i_tot} lines from {u} written to anturi_arvo')
            except Exception as e:
                log.exception('write operation failed')

        log.info('END OF SCRIPT')
    except Exception as e:
        log.exception('script interrupted')
    finally:
        if conn:
            conn.close()
        for h in log.handlers:
            h.close()

if __name__ == '__main__':
    main()
