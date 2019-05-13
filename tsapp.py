"""
Command line interface for running TSA analyses.
"""

import os
import sys
import argparse
import traceback
from pick import pick
from tsa import AnalysisCollection
from tsa.utils import trunc_str

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
                             'No valid input set',
                             'SET INPUT EXCEL FILE!'))
        else:
            ls.append(Action('Set input Excel file',
                             self.input_xlsx,
                             'Input Excel selected'))

        # 1st: select sheets
        if self.input_xlsx is None:
            ls.append(Action('Select condition sheets',
                             'No sheets selected',
                             'SET INPUT EXCEL FILE FIRST!'))
        elif not self.sheetnames:
            ls.append(Action('Select condition sheets',
                             'No sheets selected',
                             'All will be used by default.'))
        else:
            ls.append(Action('Select condition sheets',
                             f'{len(self.sheetnames)} sheets selected'))

        # 2nd: set a database parameter
        ls.append(Action('Modify database parameters',
                         self.db_params.get_status()))

        # 3rd: validate sheets
        if self.input_xlsx is None:
            ls.append(Action('Validate condition sheets',
                             'No conditions read',
                             'SET INPUT EXCEL FILE FIRST!'))
        elif not self.collections:
            ls.append(Action('Validate condition sheets',
                             'No conditions read'))
        else:
            ls.append(Action('Validate condition sheets',
                             f'{len(self.collections)} condition sets read'))

        # 4th: list errors / warnings
        ls.append(Action('List errors and warnings',
                         f'{self.n_errors} errors or warnings'))

        # 5th: set output name
        if self.name is None:
            ls.append(Action('Set output name',
                             'No output name set',
                             'Will be auto-generated'))
        else:
            ls.append(Action('Set output name',
                             self.name))

        # 6th: select output formats
        ls.append(Action('Select output formats',
                         ', '.join(self.out_formats)))

        # 7th: run analyses and save output
        if self.input_xlsx is None:
            ls.append(Action('Run & save analyses',
                             'Not ready to run',
                             'SET INPUT EXCEL FILE FIRST!'))
        elif not self.collections:
            ls.append(Action('Run & save analyses',
                             'Not ready to run',
                             'VALIDATE SHEETS FIRST!'))
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
    anls = CLIAnalysisColl()
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
            # Sheet validation
            pass # TODO
        elif idx == 4:
            # List errors / warnings
            pass
        elif idx == 5:
            # Output name selection
            pass
        elif idx == 6:
            # Output formats
            pass
        elif idx == 7:
            # Run and save analyses
            pass
        else:
            sys.exit()

if __name__ == '__main__':
    main()
