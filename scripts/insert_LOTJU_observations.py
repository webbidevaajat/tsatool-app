#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This script inserts TSA observations from LOTJU txt files
into "obs" table in tsa database.

Files to import are selected interactively from a ``filedialog``.

Database admin credentials
are needed for the insertion operations.

Note that Lotju files are assumed to have dates and times
according to local time zone 'Europe/Helsinki',
which is then converted to UTC for the database.
"""

import os
import sys
import pytz
import tkinter
import argparse
import logging
from tkinter import filedialog
from datetime import datetime
from progress.bar import ShadyBar

from tsa import tsadb_connect

# TODO: This script might be significantly more efficient
#       if implemented using Pandas data frames
#       instead of pure lists and sets.

module = sys.modules['__main__'].__file__
log = logging.getLogger(module)
file_format = logging.Formatter('%(asctime)s ; %(levelname)s ; %(message)s')
stdout_format = logging.Formatter('%(asctime)s   %(message)s')
log.setLevel(logging.INFO)
log.setLevel(logging.INFO)
sh = logging.StreamHandler()
sh.setFormatter(stdout_format)
log.addHandler(sh)

# TODO: reasonable control of logging levels

def select_lotju_files():
    """
    Select Lotju files to import
    by using a file dialog,
    return a tuple of file names.

    .. note:: Only ``.txt`` files are allowed.
    """
    root = tkinter.Tk()
    root.withdraw()
    filenames = filedialog.askopenfilenames(
        initialdir=os.getcwd(),
        filetypes=[('TXT Files (.txt)', '.txt')],
        parent=root,
        title='Select LOTJU files to import',
        multiple=True
    )
    root.destroy()
    if filenames:
        return filenames
    else:
        print('No files selected.')
        return None

def print_head_tail(ls, n=5):
    """
    Print n first and n last elements of a list,
    or entire list if len(ls) <= 2*n
    """
    if len(ls) <= 2*n:
        for el in ls:
            print(el)
    else:
        for el in ls[:n]:
            print(el)
        print(' ...')
        for el in ls[-n:]:
            print(el)

def reformat_lotju_data(infile, outfile, idnamepairs, verbose=True):
    """
    Given the ``infile`` file object of a Lotju file,
    pick and format the correct columns
    and write them into the ``outfile`` file object.

    ``idnamepairs`` should be a ``set()`` of sensor id - shortname tuples
    obtained from the database.
    These are then compared to the id-name pairs in Lotju data
    to find possible conflicts before data are inserted to the database.

    If ``verbose == True``, then the first 5 and last 5
    lines are printed before and after column modifications
    so user can ensure the correctness of data.
    """
    def format_lotju_line(line):
        """
        Format a single Lotju line
        according to the assumed indices of each element in source data,
        return a dict.
        """
        line = line.split()

        statid = int(line[1])
        seid = int(line[3])
        shortname = line[5]

        # Date + hours:minutes as str
        tfrom = '{:s} {:s}'.format(line[6], line[7])
        # Datetime object WITHOUT timestamp
        tfrom = datetime.strptime(tfrom, '%Y-%m-%d %H:%M')
        # Add info of LOCAL TIMEZONE
        tfrom = pytz.timezone('Europe/Helsinki').localize(tfrom, is_dst=None)
        # Convert to UTC
        tfrom = tfrom.astimezone(pytz.utc)
        # Convert to string
        tfrom = tfrom.strftime('%Y-%m-%d %H:%M:%S')

        seval = line[8].replace(',', '.')
        seval = float(seval)

        return dict(tfrom=tfrom,
                    statid=statid,
                    seid=seid,
                    shortname=shortname,
                    seval=seval)

    # Read, reformat and write operations are done
    # for each line, without reading the entire file to the memory
    # at any point.
    # To prevent possible error messages from filling the console,
    # find line numbers causing the errors
    # and use the last error message as a hint;
    # if there are just a few errors, print the line numbers,
    # otherwise print the number of erroneous lines.
    total_lines = sum(1 for line in infile)
    infile.seek(0)
    print(f'{total_lines} lines in total')
    i = 0  # Iterating line numbers
    j = 0   # Successfully handled line count
    errlinenrs = []
    errmessage = None
    lotju_idnamepairs = set()
    outfile.write('tfrom\tstatid\tseid\tseval\n') # Write header
    bar = ShadyBar('Handling Lotju file', max=total_lines, suffix='%(percent)d%%')
    for l in infile:
        bar.next()
        try:
            i += 1
            formatted = format_lotju_line(l)
            lotju_idnamepairs.add((formatted['seid'], formatted['shortname']))
            outline = '"{tfrom}"\t{statid}\t{seid}\t{seval}\n'.format(
                **formatted
            )
            outfile.write(outline)
            j += 1
        except Exception:
            errlinenrs.append(i)
            errmessage = sys.exc_info()[0]
    bar.finish()

    if errlinenrs:
        errlen = len(errlinenrs)
        print(f'WARNING: {errlen} erroneous lines not written.')
        if errlen <= 20:
            print('Line numbers:')
            print(', '.join(errlinenrs))
        print('Last error message:')
        print(errmessage)

    if not lotju_idnamepairs.issubset(idnamepairs):
        print('WARNING: detected id-name pairs that are')
        print('         different from those in the database:')
        print(lotju_idnamepairs.difference(idnamepairs))

    if verbose:
        print(f'{total_lines} lines read,')
        print(f'{j} lines formatted and written to new file.')

def insert_to_obs(pg_conn, cur, fileobj):
    """
    Using ``pg_conn`` and ``cursor``,
    insert contents of the file ``fileobj``
    into TSA ``obs`` table.

    First, contents are copied directly to a temporary table.
    Then the end time field ``tuntil`` is calculated for each observation.
    Observations with duration over 30 minutes are updated by
    truncating ``tuntil`` to 30 minutes from ``tfrom``,
    and number of rows affected is reported.
    Finally the results are inserted into the ``obs`` table,
    omitting last observations with ``tuntil = NULL``.
    Number of rows inserted vs total rows is reported,
    as some of the rows in the data may be in conflict with
    existing rows in the database.
    """
    statement = """
    CREATE TEMP TABLE tmp_upload
    	(tfrom timestamp,
    	statid integer,
    	seid integer,
    	seval real);
        """
    cur.execute(statement)
    pg_conn.commit()
    print('Temp table for uploading created')

    cur.copy_expert("COPY tmp_upload FROM STDIN "
                    "WITH CSV HEADER DELIMITER '\t';",
                    fileobj)
    pg_conn.commit()
    print('Data copied to temp table')
    statement = """
    CREATE TEMP TABLE tmp_obs AS (
    	SELECT
    		tfrom,
    		lead(tfrom)
    			OVER (PARTITION BY statid, seid
    				ORDER BY statid, seid, tfrom)
    			AS tuntil,
    		statid,
    		seid,
    		seval
    	FROM tmp_upload);
    """
    cur.execute(statement)
    statement = """
    WITH truncated_rows AS (
    	UPDATE tmp_obs
    		SET tuntil = tfrom + INTERVAL '30' minute
    		WHERE tuntil > tfrom + INTERVAL '30' minute
    		RETURNING 1
    	)
    SELECT COUNT(*) FROM truncated_rows;
    """
    cur.execute(statement)
    n_updated = cur.fetchone()[0]
    pg_conn.commit()
    print('End time fields calculated:')
    print(f'{n_updated} rows truncated to 30 minutes')

    cur.execute('SELECT COUNT(*) FROM tmp_obs;')
    n_toinsert = cur.fetchone()[0]
    statement = """
    WITH rows_inserted AS (
        INSERT INTO obs (tfrom, tuntil, statid, seid, seval)
        SELECT * FROM tmp_obs
        WHERE tuntil IS NOT NULL
        ON CONFLICT DO NOTHING
        RETURNING 1
        )
    SELECT COUNT(*) FROM rows_inserted;
    """
    cur.execute(statement)
    n_inserted = cur.fetchone()[0]
    pg_conn.commit()
    print(f'{n_toinsert} of {n_inserted} rows inserted into table obs.')

    cur.execute('DROP TABLE IF EXISTS tmp_obs;')
    cur.execute('DROP TABLE IF EXISTS tmp_conflicts;')
    pg_conn.commit()

def insertion_routine(interactive, rmcsv, overwrite, datadir):
    print('\nInsert LOTJU DATA')
    print('into TSA database')
    print('\n*****************\n')



    pg_conn = None
    cur = None
    try:
        pg_conn = tsadb_connect(username='tsadash')
        if not pg_conn:
            raise Exception('Db connection failed.')

        print('Connected to database.')
        cur = pg_conn.cursor()

        cur.execute("SELECT id, shortname FROM sensors ORDER BY id;")
        idnamepairs = cur.fetchall()
        idnamepairs = set(idnamepairs)
        print('{:d} sensor id-name pairs in database'.format(len(idnamepairs)))
        c = input('Press ENTER to continue to file selection...')

        lotju_files = select_lotju_files()
        if lotju_files is None:
            raise Exception('No files to operate on.')
        # For files:
        for infilename in lotju_files:
            print(f'Processing {infilename} ...')
            outfilename = infilename.replace('.txt', '_OUT.txt')
            cont = 'y'
            if os.path.exists(outfilename):
                cont = input(f'Output file {outfilename} '
                             'already exists.\n'
                             'Overwrite? [y if yes] ')
            if cont == 'y':
                with open(infilename, 'r') as infile, open(outfilename, 'w') as outfile:
                    reformat_lotju_data(infile,
                                        outfile,
                                        idnamepairs,
                                        verbose=True)
            with open(outfilename, 'r') as outfile:
                insert_to_obs(pg_conn, cur, outfile)
    except Exception as e:
        print(e)
    finally:
        if cur:
            cur.close()
        if pg_conn:
            pg_conn.close()
        print('END OF SCRIPT')

def parse_cmdline(argv):
    parser = argparse.ArgumentParser(
        description='Insert LOTJU txt file contents to TSA database.'
    )
    parser.add_argument('-i', '--interactive',
                        action='store_true',
                        default=False,
                        help='use confirmations before proceeding in the script'
                        )
    parser.add_argument('-r', '--rmcsv',
                        action='store_true',
                        default=False,
                        help='remove intermediate LOTJU csv files the script creates'
                        )
    parser.add_argument('-o', '--overwrite',
                        action='store_true',
                        default=False,
                        help='overwrite existing intermediate LOTJU csv files')
    parser.add_argument('-d', '--datadir',
                        default='data',
                        type=str,
                        help='rel or abs directory containing LOTJU files'
                        )
    parser.add_argument('-l', '--logdest',
                        default=None,
                        type=str,
                        help='destination file path for logging')
    args = parser.parse_args(argv[1:])
    return args

def main():
    try:
        args = parse_cmdline(sys.argv)
        if args['logdest']:
            fh = logging.FileHandler(args['logdest'])
            fh.setFormatter(file_format)

    except KeyboardInterrupt:
        log.error('Program interrupted')
    finally:
        logging.shutdown()

if __name__ == '__main__':
    main()