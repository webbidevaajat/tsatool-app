"""
Script for running TSA analyses as batch job.
Meant for background use: all input and output parameters
are assumed clean and valid, and there are no interactive
wait / confirmation steps!
"""
import os
import sys
import argparse
import psycopg2
import logging
import logging.handlers
import logging.config
from tsa import AnalysisCollection

log = logging.getLogger('tsa')
log.setLevel(logging.DEBUG)
fh = logging.handlers.TimedRotatingFileHandler(os.path.join('logs', 'tsabatchlog'))
fh.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s; %(name)s; %(levelname)s; %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
log.addHandler(fh)
log.addHandler(ch)

def main():
    log.debug('START OF TSABATCH SESSION')
    parser = argparse.ArgumentParser(description='Run TSA analyses as batch job.')
    parser.add_argument('-i', '--input',
                        type=str,
                        help='File name of the input Excel file in analysis/',
                        metavar='INPUT_XLSX_NAME',
                        required=True)
    parser.add_argument('-n', '--name',
                        type=str,
                        help='Base name for the outputs in analysis/',
                        metavar='OUTPUT_NAME',
                        required=True)
    parser.add_argument('-p', '--password',
                        type=str,
                        help='Database password (of the user in db_config.json file)',
                        metavar='DB_PASSWORD',
                        required=True)
    args = parser.parse_args()
    log.debug(f'Started with input={args.input} name={args.name} password=***')

    anls = AnalysisCollection(name=args.name)
    try:
        anls.set_input_xlsx(path=os.path.join(anls.data_dir, args.input))
    except:
        log.exception('Could not set input Excel file name.')
        sys.exit()

    # Read DB params from config file, add password
    try:
        anls.db_params.read_config('db_config.json')
        anls.db_params.password = args.password
    except:
        log.exception('Could not find DB config file.')
        sys.exit()

    # Add all sheet names for analysis
    sheets = anls.workbook.sheetnames
    log.info(f"Using all sheets: {', '.join(sheets)}")
    anls.set_sheetnames(sheets=sheets)

    # Prepare and validate collections
    try:
        log.info('Connecting to database ...')
        with psycopg2.connect(**anls.db_params, connect_timeout=5) as conn:
            log.info('Fetching available station ids ...')
            anls.save_statids_in_statobs(conn)
            log.info('Fetching sensor name-id pairs ...')
            anls.save_sensor_pairs(conn)
        log.info(f'Fetched {len(anls.statids_in_db)} station ids and {len(anls.sensor_pairs)} sensor name-id pairs.')
    except:
        log.exception('Error with database connection.')
        sys.exit()
    for s in anls.sheetnames:
        try:
            log.info(f'Adding collection "{s}" ...')
            anls.add_collection(s)
        except Exception as e:
            log.exception(f'Error when adding collection {s}.')
            anls.add_error(e)
    log.info(f'{len(anls.collections)} collections added from {len(anls.sheetnames)} sheets.')
    if anls.statids_in_db:
        log.info('Validating collection station ids.')
        n_errs = anls.check_statids()
        if n_errs:
            log.warning(f'There were {n_errs} errors.')
    else:
        err = 'Could not check if station ids exist in database'
        log.warning(err)
        anls.add_error(err)
    errs = anls.list_errors()
    if errs:
        log.warning(f'{len(errs)} errors:\n' + '\n'.join(errs))
        outpath = os.path.join(anls.get_outdir(), 'errors.txt')
        with open(outpath, 'w') as fobj:
            fobj.write('\n'.join(errs))
        log.info(f'Errors saved to {outpath}.')

    # Analyze and save
    anls.run_analyses()

    log.debug('END OF TSABATCH SESSION')

if __name__ == '__main__':
    main()
