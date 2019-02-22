#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Run this script after cloning the Github repo
to create necessary directories and config files.
"""

import os

def main():
    cwd = os.getcwd()

    dirpath = os.path.join(cwd, 'data')
    if not os.path.exists(dirpath):
        os.mkdir(dirpath)
        print(f'{dirpath} created')
    else:
        print(f'{dirpath} already exists')

    dirpath = os.path.join(cwd, 'logs')
    if not os.path.exists(dirpath):
        os.mkdir(dirpath)
        print(f'{dirpath} created')
    else:
        print(f'{dirpath} already exists')

    cfpath = os.path.join(cwd, 'db_config.json')
    if not os.path.exists(cfpath):
        cf_str = '''{
  "HOST": "localhost",
  "PORT": 5432,
  "DATABASE": "tsa",
  "ADMIN_USER": "tsadash",
  "ORDINARY_USERS": []
}'''
        with open(cfpath, 'w') as outfile:
            outfile.write(cf_str)
        print(f'{cfpath} created with default values')
    else:
        print(f'{cfpath} already exists')

if __name__ == '__main__':
    main()
