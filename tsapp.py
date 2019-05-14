"""
Command line interface for running TSA analyses.
"""

import os
import sys
import yaml
import argparse
import traceback
import psycopg2
import logging
import logging.config
from pick import pick
from tsa import AnalysisCollection
from tsa.utils import trunc_str

with open('logging_config.yml', 'r') as f:
    config = yaml.safe_load(f.read())
    logging.config.dictConfig(config)
    log = logging.getLogger('tsapplogger')

class Action:
    """
    Template for CLI actions / choices related to CLIAnalysisColl,
    with some informative attributes.
    """
    def __init__(self, title, content, message=''):
        self.title = title
        self.content = content
        self.message = message

    def __str__(self):
        """
        Pick-compatible string representation
        """
        cnt = f'[{self.content}]'
        if not self.message:
            return '{:28} {:67}'.format(trunc_str(self.title, n=33),
                                        trunc_str(cnt, n=67))
        else:
            return '{:28} {:33} {:33}'.format(trunc_str(self.title, n=33),
                                              trunc_str(cnt, n=33),
                                              trunc_str(self.message, n=33))

class CLIAnalysisColl(AnalysisCollection):
    """
    Extended version for interactive CLI use.
    """

    def list_main_actions(self):
        """
        Return list of main actions on the analysis collection,
        along with the related statuses to be prompted.
        This is to be used with the interactive CLI main menu.
        """
        ls = []

        # 0th: input excel filename
        if self.input_xlsx is None:
            ls.append(Action('Set input Excel file',
                             'No valid input set. Set input Excel first!'))
        else:
            ls.append(Action('Set input Excel file',
                             self.input_xlsx))

        # 1st: select sheets
        if self.input_xlsx is None:
            ls.append(Action('Select condition sheets',
                             'Set input Excel first!'))
        elif not self.sheetnames:
            ls.append(Action('Select condition sheets',
                             'No sheets selected: all will be used by default.'))
        else:
            ls.append(Action('Select condition sheets',
                             f'{len(self.sheetnames)} sheets selected'))

        # 2nd: set a database parameter
        ls.append(Action('Modify database parameters',
                         self.db_params.get_status()))

        # 3rd: validate sheets
        if self.input_xlsx is None:
            ls.append(Action('Validate condition sheets',
                             'No conditions read. Set input Excel first!'))
        elif not self.collections:
            ls.append(Action('Validate condition sheets',
                             'No conditions read'))
        else:
            ls.append(Action('Validate condition sheets',
                             f'{len(self.collections)} condition sets read'))

        # 4th: list errors / warnings
        ls.append(Action('List errors and warnings',
                         f'{len(self.list_errors())} errors or warnings'))

        # 5th: set output name
        if self.name is None:
            ls.append(Action('Set output name',
                             'No output name set: will be auto-generated.'))
        else:
            ls.append(Action('Set output name',
                             self.name))

        # 6th: select output formats
        ls.append(Action('Select output formats',
                         ', '.join(self.out_formats)))

        # 7th: run analyses and save output
        if self.input_xlsx is None:
            ls.append(Action('Run & save analyses',
                             'Not ready to run: set input Excel first!'))
        elif self.db_params.get_status() != 'DB params ready':
            ls.append(Action('Run & save analyses',
                             'Not ready to run: prepare DB parameters first!'))
        elif not self.collections:
            ls.append(Action('Run & save analyses',
                             'Not ready to run: validate sheets first!'))
        elif self.errmsgs:
            ls.append(Action('Run & save analyses',
                             'Ready to run (there are errors)'))
        else:
            ls.append(Action('Run & save analyses',
                             'Ready to run'))

        # Last index: exit program
        ls.append(Action('Exit program', ''))

        return ls

    def cli_set_input_xlsx(self):
        """
        Let user set the input file from a Pick menu.
        Return next main menu default index accordingly.
        """
        def folders_xlsx(dir):
            """
            List directories and Excel files in a directory,
            and add parent directory and exit options.
            """
            ls = os.listdir(dir)
            ls = [el for el in ls if el.endswith('.xlsx') or os.path.isdir(el)]
            ls.append('..')
            ls.append('(exit)')
            return ls
        try:
            d = self.base_dir
            if os.path.exists(self.data_dir):
                d = self.data_dir
            sel = ''
            while not sel.endswith('.xlsx'):
                opts = folders_xlsx(d)
                sel, i = pick(options=opts,
                              title='Select directory or Excel file')
                if sel.endswith('.xlsx'):
                    sel = os.path.join(d, sel)
                    self.set_input_xlsx(sel)
                    return 1
                elif sel == '(exit)':
                    return 0
                else:
                    d = os.path.normpath(os.path.join(d, sel))
        except:
            traceback.print_exc()
            input('(press ENTER to continue)')
            return 0

    def cli_set_sheetnames(self):
        """
        Get workbook sheet names and set sheets to analyze interactively.
        """
        if self.workbook is None:
            print('No Excel workbook. Select the input file first.')
            input('(press ENTER to continue)')
            return 0
        try:
            opts = self.workbook.sheetnames
            sel = pick(options=opts,
                       title='Select sheets to analyze (SPACE) or select nothing to include all',
                       multi_select=True)
            if sel:
                sel = [el[0] for el in sel]
            else:
                sel = opts
            self.set_sheetnames(sheets=sel)
            return 2
        except:
            traceback.print_exc()
            input('(press ENTER to continue)')
            return 0

    def cli_set_db_parameter(self):
        """
        Set a single db parameter interactively.
        """
        def printrow(t):
            return f'{t[0]:9} [{t[1]}]'
        opts = self.db_params.as_tuples()
        opts.append(('(exit)', ''))
        param, i = pick(options=opts,
                        title='Select a parameter to modify',
                        options_map_func=printrow)
        if param[0] == '(exit)':
            return 2
        self.db_params.set_value_interactively(param[0])
        if self.db_params.get_status() == 'DB params ready':
            return 3
        return 2

    def cli_prepare_collections(self):
        """
        Get available station ids from database,
        get sensor name-id pairs from database,
        read in collections from workbook sheets,
        and check if their station ids exist in the db.
        """
        if self.db_params.get_status() != 'DB params ready':
            print('ERROR: Set database parameters first!')
            _ = input('(Hit ENTER to continue)')
            return 2
        if self.workbook is None:
            print('ERROR: Select Excel file first!')
            _ = input('(Hit ENTER to continue)')
            return 0
        print('Going to fetch available station ids and sensor name-id pairs from database.')
        go = input('(Hit ENTER to run or type "exit" and hit ENTER to cancel)')
        if go.startswith('e'):
            return 3
        # On no selected sheetnames, set them to all available
        if not self.sheetnames:
            print('No sheetnames selected, using all available sheets.')
            self.sheetnames = self.workbook.sheetnames
        try:
            print('Connecting to database ...')
            with psycopg2.connect(**self.db_params, connect_timeout=5) as conn:
                print('Fetching available station ids ...')
                self.save_statids_in_statobs(conn)
                print('Fetching sensor name-id pairs ...')
                self.save_sensor_pairs(conn)
            print(f'Fetched {len(self.statids_in_db)} station ids and {len(self.sensor_pairs)} sensor name-id pairs.')
        except:
            traceback.print_exc()
            print('(press ENTER to continue without station ids and sensor pairs')
            go = input(' or type "exit" and hit ENTER to exit)')
            if len(go) > 0 and go.startswith('e'):
                return 3
        # Add selected worksheet contents, pass erroneous ones but log an error
        for s in self.sheetnames:
            try:
                print(f'Adding collection "{s}" ...')
                self.add_collection(s)
            except Exception as e:
                traceback.print_exc()
                self.add_error(e)
                _ = input('(Hit ENTER to continue)')
        print(f'{len(self.collections)} collections added from {len(self.sheetnames)} sheets.')
        if self.statids_in_db:
            print('Going to validate collection station ids.')
            _ = input('(Hit ENTER to continue)')
            n_errs = self.check_statids()
            if n_errs:
                print(f'There were {n_errs} errors.')
                print('Select "List errors and warnings" to see them.')
        else:
            err = 'WARNING: could not check station if ids exist in database'
            print(err)
            self.add_error(err)
        _ = input('(Hit ENTER to exit)')
        return 4

    def cli_list_errors(self):
        """
        List current errors interactively or into a file.
        """
        errs = self.list_errors()
        if not errs:
            sel, i = pick(options=['(exit)'],
                          title='No errors or warnings')
        else:
            opts = ['Print errors and warnings interactively',
                    'Save errors and warnings into file',
                    '(exit)']
            title = f'{len(errs)} errors or warnings'
            sel, i = pick(options=opts,
                          title=title)
        if i == 0:
            # Print interactively, exit if given sth starting with 'e'
            for i, err in enumerate(errs):
                nxt = input(f'{i+1}/{len(errs)} {err}')
                if len(nxt) > 0 and nxt[0] == 'e':
                    break
            return 4
        elif i == 1:
            outpath = os.path.join(self.get_outdir(), 'errors.txt')
            alt = input(f'Output file path [default: {outpath}]: ')
            if alt:
                outpath = alt
            try:
                with open(outpath, 'w') as fobj:
                    fobj.write('\n'.join(errs))
            except:
                traceback.print_exc()
                input('(press ENTER to continue)')
            finally:
                return 4
        else:
            return 4

    def cli_set_name(self):
        """
        Set analysis name that is used
        in output folder and file names.
        """
        print('Enter analysis name (will be made into valid file / dir name)')
        newname = input(f'[hit ENTER to keep current name {self.name}]: ')
        if newname:
            self.name = newname
        return 5

    def cli_set_output_formats(self):
        """
        Set output formats of analysis results.
        """
        def printrow(t):
            return f'{t[0]:5} [{t[1]}]'
        opts = [('xlsx', 'Excel with summary results (similar to input Excel)'),
                ('pptx', 'PowerPoint presentation from each condition collection'),
                ('log', 'Save a debugging & error log')]
        title = ('Select (SPACE) output formats and hit ENTER\n'
                 f'or select nothing to keep current formats [{", ".join(self.out_formats)}]')
        sel = pick(options=opts,
                   title=title,
                   multi_select=True,
                   options_map_func=printrow)
        if sel:
            sel = [el[0][0] for el in sel]
            self.out_formats = sel
        return 6

def main():
    parser = argparse.ArgumentParser(description='Prepare, validate and run TSA analyses interactively.')
    parser.add_argument('-i', '--input',
                        type=str,
                        help='Excel file name from which condition collections are read',
                        metavar='INPUT_XLSX_NAME')
    parser.add_argument('-n', '--name',
                        type=str,
                        help='Name of the output folder / zip / xlsx file',
                        metavar='OUTPUT_NAME')
    args = parser.parse_args()

    # TODO: take cl arguments into account
    anls = CLIAnalysisColl(input_xlsx=args.input,
                           name=args.name)
    defidx = 0
    maintitle = (f'   {"#"*15}\n'
                 '   TSAPP main menu\n'
                 f'   {"#"*15}')

    # Read DB params from config file by default
    try:
        anls.db_params.read_config('db_config.json')
    except:
        print('Could not find DB config file:')
        traceback.print_exc()
        input('(press ENTER to continue)')

    while True:
        ########################
        # This is the main menu.
        ########################
        if defidx is None:
            defidx = 0
        act, idx = pick(options=anls.list_main_actions(),
                        title=maintitle,
                        indicator='=>',
                        default_index=defidx,
                        options_map_func=str)

        if idx == 0:
            # Input excel filename selection
            defidx = anls.cli_set_input_xlsx()
        elif idx == 1:
            # Sheet selection
            defidx = anls.cli_set_sheetnames()
        elif idx == 2:
            # Database parameters
            defidx = anls.cli_set_db_parameter()
        elif idx == 3:
            # Sheet preparation (read & check station id existence)
            defidx = anls.cli_prepare_collections()
        elif idx == 4:
            # List errors / warnings
            defidx = anls.cli_list_errors()
        elif idx == 5:
            # Output name selection
            defidx = anls.cli_set_name()
        elif idx == 6:
            # Output formats
            defidx = anls.cli_set_output_formats()
        elif idx == 7:
            # Run and save analyses
            pass
        else:
            sys.exit()

if __name__ == '__main__':
    main()
