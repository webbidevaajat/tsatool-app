#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This script inserts TSA observations from LOTJU txt files
into "obs" table in tsa database.

Files to import are selected interactively from a ``filedialog``.

Note that ``db_config.json`` file must be available
to connect to the database. Database admin credentials
are needed for the insertion operations.
"""

import os
import tkinter
from tkinter import filedialog
#
# - statid, idx 1
# - seid, idx 3 or 4???
# - sensor name, idx 5; compare to id?
# - date idx 6 + mm:ss idx 7 -> datetime AT TZ Europe/Helsinki
# - sval, idx 8 -> replace , by .; convert to float

def select_lotju_files():
    """
    Select Lotju files to import
    by using a file dialog,
    return a tuple of file names.
    """
    root = tkinter.Tk()
    root.withdraw()
    filenames = filedialog.askopenfilenames(
        initialdir=os.getcwd(),
        filetypes=[('text files', '.txt')],
        parent=root,
        title='Select LOTJU files to import',
        multiple=True
    )
    root.destroy()
    if filenames:
        print('Importing following files:')
        for fn in filenames:
            print('- {:s}'.format(fn))
        return filenames
    else:
        print('No files selected.')
        return None

def main():
    print('\nInsert LOTJU DATA')
    print('into TSA database')
    print('\n*****************\n')
    with open('db_config.json', 'r') as cf_file:
        cf = json.load(cf_file)
    pswd = getpass('Password for user "{:s}":'.format(cf['ADMIN_USER']))
    try:
        pg_conn = pg.connect(dbname=cf['DATABASE'],
                             user=cf['ADMIN_USER'],
                             password=pswd,
                             host=cf['HOST'],
                             port=cf['PORT'],
                             connect_timeout=5)
    except pg.OperationalError as e:
        print('Could not connect to database:')
        print(e)
        print('Are you connected to the right network?')
        sys.exit()

    print('Connected to database.')
    cur = pg_conn.cursor()

    lotju_files = select_lotju_files()

    # For files:
    #   TODO data read and format
    #   TODO data insert

    # TODO result summary

    cur.close()
    pg_conn.close()
    print('END OF SCRIPT')

# if __name__ == '__main__':
#     main()
