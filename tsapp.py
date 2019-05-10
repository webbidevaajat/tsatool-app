"""
Command line interface for running TSA analyses.
"""

import sys
import argparse
import tsa
from pick import pick

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

    anls = tsa.AnalysisCollection(input_xlsx=args.input,
                                  name=args.name)
    defidx = 0
    maintitle = f'{"#"*15}\nTSAPP main menu\n{"#"*15}'

    while True:
        ########################
        # This is the main menu.
        ########################
        act, idx = pick(options=anls.list_main_actions(),
                        title=maintitle,
                        indicator='=>',
                        default_index=defidx,
                        options_map_func=str)

        print(act.__dict__)
        if idx == 7:
            sys.exit()

if __name__ == '__main__':
    main()
