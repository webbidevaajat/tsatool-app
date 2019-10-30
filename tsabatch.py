#!env/bin/python

"""
Script for running TSA analyses as batch job.
Meant for background use: all input and output parameters
are assumed clean and valid, and there are no interactive
wait / confirmation steps!
"""
import os
import re
import sys
import argparse
import psycopg2
import logging
from tsa.analysis_collection import AnalysisCollection
from tsa.utils import list_local_statids
from tsa.utils import list_local_sensors

def main():
    # ---- LOGGING ----
    os.makedirs('logs', exist_ok=True)
    log = logging.getLogger()
    log.setLevel(logging.DEBUG)
    fh = logging.FileHandler(os.path.join('logs', 'tsabatchlog'))
    ch = logging.StreamHandler()
    fh.setFormatter(
        logging.Formatter(
            '%(asctime)s; %(module)-20s; line %(lineno)-3d; %(levelname)-8s; %(message)s'
            )
        )
    ch.setFormatter(logging.Formatter('%(module)-20s; line %(lineno)-3d; %(levelname)-8s; %(message)s'))
    log.addHandler(fh)
    log.addHandler(ch)

    # ---- COMMAND LINE ARGUMENTS ----
    parser = argparse.ArgumentParser(description='Run TSA analyses as batch job.')
    parser.add_argument('-i', '--input',
                        type=str,
                        help='Input Excel path relative to script directory',
                        metavar='INPUT_XLSX_PATH',
                        required=True)
    parser.add_argument('-n', '--name',
                        type=str,
                        help='Base name for output files saved under results/',
                        metavar='OUTPUT_BASENAME',
                        required=True)
    parser.add_argument('--dryvalidate',
                        action='store_true',
                        help='Only validate input Excel with hard-coded ids and names')
    args = parser.parse_args()
    if args.name is None:
        # Use input excel name but replace file ending
        args.name = re.sub("\.[^.]*$", "_OUT", args.input)
    log.info((f'START OF TSABATCH with input={args.input} name={args.name} '
              f'dryvalidate={args.dryvalidate}'))

    # ---- APP LOGIC ----
    anls = AnalysisCollection(input_xlsx=args.input, name=args.name)
    log.info(f'Created {str(anls)}')

    # Add all sheets for analysis ("info" is omitted by default)
    anls.add_collections()
    sys.exit()

    if args.dryvalidate:
        log.debug(f"Validating input Excel without database")
        anls.statids_in_db = set(list_local_statids())
        anls.sensor_pairs = list_local_sensors()
        for s in anls.sheetnames:
            anls.add_collection(title=s)
        errs = anls.list_errors()
        if errs:
            fname = os.path.join("logs", "excel_validation_report.txt")
            log.warning(f'There were errors in input Excel: see {fname}')
            with open(fname, "w") as fobj:
                fobj.write('\n'.join(errs))
            # Error raised -> things outside the Python process
            # can determine further actions
            raise Exception("Errors in input Excel file")
        else:
            log.info('No errors in input Excel')
        # With dryvalidate, the script is exited anyway
        # without proceeding to DB communication and analysis
        sys.exit()

    # Prepare and validate collections
    try:
        log.debug('Connecting to database ...')
        with psycopg2.connect(**anls.db_params, connect_timeout=5) as conn:
            log.debug('Fetching available station ids ...')
            anls.save_statids_in_statobs(conn)
            log.debug('Fetching sensor name-id pairs ...')
            anls.save_sensor_pairs(conn)
        log.info(f'Fetched {len(anls.statids_in_db)} station ids and {len(anls.sensor_pairs)} sensor name-id pairs')
    except:
        log.exception('Error with DB when fetching station and sensor ids: quitting')
        sys.exit()

    for s in anls.sheetnames:
        try:
            log.debug(f'Adding sheet {s} ...')
            anls.add_collection(s)
        except Exception as e:
            log.error(f'Failed to add sheet {s} contents to analysis collection',
                      exc_info=True)
            anls.add_error(e)
    log.info(f'{len(anls.collections)} collections added from {len(anls.sheetnames)} sheets')
    if anls.statids_in_db:
        log.debug('Validating collection station ids')
        n_errs = anls.check_statids()
        if n_errs:
            log.warning(f'There were {n_errs} errors.')
    else:
        log.error('Could not check if station ids exist in database')
        anls.add_error(err)
    errs = anls.list_errors()
    if errs:
        log.error(f'{len(errs)} errors with analysis collection {anls.name}')
        log.debug('Listing errors:')
        for e in errs:
            log.debug(f'    {e}')

    # Analyze and save
    anls.run_analyses()

    log.info('END OF TSABATCH')

if __name__ == '__main__':
    main()
