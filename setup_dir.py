#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Run this script after cloning the Github repo
to create necessary directories and config files.
"""

import os
import yaml
import logging
import logging.config

with open('logging_config.yml', 'r') as f:
    config = yaml.safe_load(f.read())
    logging.config.dictConfig(config)
    log = logging.getLogger('setuplogger')

def main():
    cwd = os.getcwd()
    log.info(f'Setting up {cwd}')

    for folder in ['data', 'logs', 'analysis']:
        dirpath = os.path.join(cwd, folder)
        if not os.path.exists(dirpath):
            os.mkdir(dirpath)
            log.info(f'{dirpath} created')
        else:
            log.info(f'{dirpath} already exists')

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
        log.info(f'{cfpath} created with default values')
    else:
        log.info(f'{cfpath} already exists')

if __name__ == '__main__':
    main()
