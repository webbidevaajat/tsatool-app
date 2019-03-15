#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Copy csv raw data file of anturi_arvo
to tsa database table anturi_arvo.

Give csv file path as argument.
"""
import logging
import sys
import os
from io import StringIO
from tsa import tsadb_connect

log = logging.getLogger(__name__)
formatter = logging.Formatter('%(asctime)s; %(levelname)s; %(message)s',
                              '%Y-%m-%d %H:%M:%S')
fh = logging.FileHandler(os.path.join('logs', 'anturi_insertions.log'))
fh.setFormatter(formatter)
log.addHandler(fh)
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(logging.Formatter('%(asctime)s; %(message)s'))
log.addHandler(ch)
log.setLevel(logging.DEBUG)

def main():
    log.info('START OF SCRIPT')
    fp = sys.argv[1]
    fp = os.path.abspath(fp)
    assert os.path.exists(fp)

    conn = tsadb_connect(username='tsadash', password='PASSWORD_HERE')
    cur = conn.cursor()

    chksize = 10000000
    totlines = 0
    output = StringIO()

    try:
        with open(fp, 'r') as f:
            # Ignore header line
            line = f.readline()
            line = f.readline()
            cnt = 0
            chunk = []
            while line:
                cnt += 1
                totlines += 1
                el = line.strip()
                if el.endswith('|'):
                    el = el[:-1] + '\n'
                output.write(el)
                line = f.readline()
                if cnt == chksize or not line:
                    output.seek(0)
                    for l in output:
                        log.info(f'{totlines} inserted; inserting from {l.strip()}')
                        break
                    output.seek(0)
                    cur.copy_from(file=output,
                                  table='anturi_arvo',
                                  sep='|')
                    conn.commit()
                    output.close()
                    output = StringIO()
                    cnt = 1

    except Exception as e:
        print(e)
    finally:
        log.info('END OF SCRIPT')
        cur.close()
        conn.close()
        output.close()
        for h in log.handlers:
            h.close()

if __name__ == '__main__':
    main()
