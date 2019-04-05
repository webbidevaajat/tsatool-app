#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ARGUMENT: month number from which on
the station observations are included:
leave out observations that are already in the database.

Form subsets of sensor observations
based on a manually defined station id set,
replace old sensor ids with new ones
and insert the per-station-chunk into ``seobs`` table.
For each insertion, count the number of rows inserted.

Finally, an index on ``seobs(obsid)`` is created.

NOTE: this is a very time-consuming operation
as ``anturi_arvo`` table has billions of rows and no indexes!
"""
import psycopg2 as pg
import logging
import sys
import os
from tsa import tsadb_connect

log = logging.getLogger(__name__)
formatter = logging.Formatter('%(asctime)s; %(levelname)s; %(message)s',
                              '%Y-%m-%d %H:%M:%S')
fh = logging.FileHandler(os.path.join('logs', 'seobs_insertions.log'))
fh.setFormatter(formatter)
log.addHandler(fh)
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(logging.Formatter('%(message)s'))
log.addHandler(ch)
log.setLevel(logging.DEBUG)

# tsadash password
PWD = 'PWD_HERE'

# Station ids have been picked from the database "ad hoc".
STATIDS = [
1006, 1011, 1019, 1022, 1083, 1101, 1103, 1104, 1105, 1106, 1107,
1108, 1109, 1110, 1111, 1115, 1116, 1118, 1119, 1120, 1121, 1122,
1123, 1124, 1125, 1126, 1131, 1132, 3001, 3002, 3006, 3023, 3024,
3029, 3030, 3045, 3047, 3048, 3051, 3052, 3053, 3054, 3056, 3057,
3058, 3059, 3062, 3063, 3064, 3065, 3066, 3067, 3074, 3075, 3077,
3078, 3079, 3080, 3081, 3082
]

# The following string handles the huge insertions per station id
# and will get each station id integer as a parameter.
INS_SQL = """
-- seids handles replacing old sensor ids with new ones
WITH seids AS (
	SELECT laskennallinen_anturi.id AS lotjuid,
	sensors.id AS dtid
	FROM sensors
	INNER JOIN laskennallinen_anturi
	ON sensors.id = laskennallinen_anturi.vanha_id
),
-- obsid_subset is defined for each given station id
obsid_subset AS (
	SELECT id
	FROM statobs
	WHERE statid = {:d}
    AND tfrom >= '2018-{:02d}-01 00:00:00'
),
-- Form subset of sensor observations for insertion:
anids AS (
	SELECT anturi_arvo.id AS id,
		anturi_arvo.mittatieto_id AS obsid,
		seids.dtid AS seid,
		anturi_arvo.arvo AS seval
	FROM anturi_arvo
    -- Filter by station observation subset
	INNER JOIN obsid_subset
	ON obsid_subset.id = anturi_arvo.mittatieto_id
    -- Add new sensor ids and only use values that actually
    -- have a corresponding new sensor id (that is why INNER join)
	INNER JOIN seids
	ON seids.lotjuid = anturi_arvo.anturi_id
)

INSERT INTO seobs (id, obsid, seid, seval)
	SELECT anids.id AS id,
		anids.obsid AS obsid,
		anids.seid AS seid,
		anids.seval AS seval
	FROM anids
	ON CONFLICT DO NOTHING;
"""

def main():
    month_nr = int(sys.argv[1])
    log.info('Starting sensor observation insertions')
    conn = None
    try:
        conn = tsadb_connect(username='tsadash', password=PWD)
        cur = conn.cursor()
        log.info(f'Using statobs from month {month_nr} on')
        for statid in STATIDS:
            try:
                log.info(f'Inserting with statid {statid}')
                sql_cmd = INS_SQL.format(statid, month_nr)
                cur.execute(sql_cmd)
                conn.commit()
                log.info(f'inserted with statid {statid}')
            except Exception as e:
                conn.rollback()
                log.exception(f'Could not insert with statid {statid}')
                continue
        conn.commit()
    except Exception as e:
        log.exception('script interrupted')
    finally:
        log.info('END OF SCRIPT')
        if conn:
            conn.close()
        for h in log.handlers:
            h.close()

if __name__ == '__main__':
    main()
