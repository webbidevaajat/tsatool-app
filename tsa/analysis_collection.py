#!/usr/bin/python
# -*- coding: utf-8 -*-

# Collection of CondCollections

import logging
import os
import re
import yaml
import psycopg2
import openpyxl as xl
from .cond_collection import CondCollection
from .utils import trunc_str
from .utils import list_local_statids()
from .utils import list_local_sensors()
from .tsaerror import TsaError
from datetime import datetime
from getpass import getpass
from collections import OrderedDict

log = logging.getLogger(__name__)

class DBParams:
    """
    Stores parameters for database connection.
    """
    def __init__(self, dbname=None, user=None, host=None, port=5432):
        self.dbname = dbname
        self.user = user
        self.password = os.getenv('POSTGRES_PASSWORD')
        self.host = host
        self.port = port

    def read_config(self, path='db_config.yml'):
        """
        Read and set params from file (except password)
        """
        with open(path, 'r') as f:
            cf = yaml.safe_load(f.read())
        self.dbname = cf['database']
        self.user = cf['admin_user']
        self.host = cf['host']
        self.port = cf['port']

    def set_value_interactively(self, par):
        if par == 'dbname':
            self.dbname = input('Database name: ')
        elif par == 'user':
            self.user = input('Database user: ')
        elif par == 'password':
            self.password = getpass('Database password: ')
        elif par == 'host':
            self.host = input('Database host: ')
        elif par == 'port':
            self.port = int(input('Database port number: '))
        else:
            raise Exception(f'Unknown DB parameter {par}')

    def keys(self):
        return ['dbname', 'user', 'password', 'host', 'port']

    def as_tuples(self):
        placeholder = 'no password set'
        if self.password is not None:
            placeholder = '*' * len(self.password)
        d = [('dbname', self.dbname),
             ('user', self.user),
             ('password', placeholder),
             ('host', self.host),
             ('port', str(self.port))]
        return d

    def get_status(self):
        missing = []
        for k in self.keys():
            if self.__dict__[k] is None:
                missing.append(k)
        if not missing:
            return 'DB params ready'
        else:
            return 'Missing {}'.format(', '.join(missing))

    def __getitem__(self, key):
        return self.__dict__[key]

    def __str__(self):
        s = 'DBParams\n'
        for k in self.keys():
            if k == 'password' and self.password is not None:
                v = '*'*len(self.password)
            else:
                v = self.__dict__[k]
            s += f'{k:8}: {v}\n'
        return s

class AnalysisCollection:
    """
    A collection of ``CondCollection`` instances.
    Can be used to run multiple analyses as a batch job.
    Enables validating CondCollections separately
    and then analysing them.

    The collection is based on an Excel file,
    and each worksheet produces a CondCollection (unless valid).
    The results (common xlsx file, and a pptx file for each CondCollection)
    are saved into a new directory named after ``name``,
    or after the Excel filename if no name given,
    and they can be optionally zipped.

    .. note: Existing files with same filepath will be overwritten.
    """

    def __init__(self, input_xlsx, name=None):
        self.input_xlsx = input_xlsx
        # If no name given, use input xlsx path buth without
        # file ending and directories
        self._name = name or re.match("[^\\\/.]+(?=\.[^_.]*$)", input_xlsx)[0]
        self.workbook = xl.load_workbook(filename=input_xlsx,
                                         read_only=True)
        self.sheetnames = self.workbook.sheetnames
        self.collections = OrderedDict()
        self.errors = list()
        # Hard-coded ids and name-id pairs as default
        self.statids_available = set(list_local_statids())
        self.sensors_available = dict(list_local_sensors())
        self.db_params = DBParams()

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, newname):
        """
        Ensure the name can be used as file and dir name.
        """
        newname = str(newname)
        newname = newname.strip()
        newname = newname.replace(' ', '_')
        self._name = ''.join([c for c in newname if c.isalnum()])

    def add_error(self, msg, lvl='Error'):
        """
        Add error message to error message list.
        Only unique errors are collected, in order to avoid
        piling up repetitive messages from loops, for example.
        """
        err = TsaError(lvl=lvl, cxt=f'Analysis {self.name}', msg=msg)
        if err not in self.errors:
            self.errors.append(err)

    def list_all_errors(self):
        """
        Collect together all error and warning messages
        of condition collections and their conditions,
        return as list of TsaError objects.
        """
        errs = [e for e in self.errors]
        for coll in self.collections.values():
            suberrs = [e for e in coll.errors]
            errs.extend(suberrs)
            for cond in coll.conditions:
                suberrs = [e for e in cond.errors]
                errs.extend(suberrs)
        return errs

    def add_collection(self, title):
        """
        Add a CondCollection from Excel sheet by its title,
        without db connection.
        """
        self.collections[title] = CondCollection.from_xlsx_sheet(
            ws=self.workbook[title],
            pg_conn=None,
            station_ids=self.statids_available,
            sensor_pairs=self.sensors_available)

    # TODO: Do we need sensor checking from database?
    # def set_sensors_available_from_db(self, pg_conn):
    #     """
    #     Get and set available sensor name-id pairs from database.
    #     ``pg_conn`` must be a valid connection object to TSA db.
    #     """
    #     try:
    #         with pg_conn.cursor() as cur:
    #             cur.execute("SELECT lower(name) AS name, id FROM sensors;")
    #             tb = cur.fetchall()
    #             self.sensors_available = {k:v for k, v in tb}
    #     except:
    #         msg = 'Could not get sensors from database'
    #         self.add_error(msg=msg)
    #         log.error(msg, exc_info=True)

    # TODO: do we need station id checking from database?
    # def save_statids_in_statobs(self, pg_conn=None, ids=None):
    #     """
    #     Get all station ids that are generally available in observation data,
    #     or give id list from outside.
    #     ``pg_conn`` must be a valid, online connection instance to TSA db.
    #     """
    #     if pg_conn is not None:
    #         with pg_conn.cursor() as cur:
    #             sql = "SELECT DISTINCT statid FROM statobs ORDER BY statid;"
    #             cur.execute(sql)
    #             statids = cur.fetchall()
    #             statids = [el[0] for el in statids]
    #             self.statids_in_db = set(statids)
    #     elif ids is not None:
    #         self.statids_in_db = set(ids)
    #     else:
    #         raise Exception("No pg_conn or station ids provided")

    def dry_validate(self):
        """
        Validate input syntax, ids and sensor names without database,
        using hard-coded station ids and sensor name-id pairs.
        On complete success, return empty string;
        on any error, return error log string.
        """
        pass

    def run_analyses(self):
        """
        Run analyses for all collections and save results.
        Analyses are run against collection-specific db connections.
        """
        log.info(f'START OF ANALYSES for analysis collection {self.name}')

        if len(self.collections) == 0:
            log.critical('No collections to analyze, quitting')
            sys.exit()

        pptx_template_path = os.path.join('data', 'report_template.pptx')
        if not os.path.exists(pptx_template_path):
            log.critical(f'PowerPoint report template {pptx_template_path} does not exist, quitting')
            sys.exit()

        out_dir = os.path.join('analysis', self.name)
        if os.path.exists(out_dir):
            old = out_dir
            out_dir += 'N'
            msg = f'Output dir {old} already exists, making it to {out_dir}'
            self.add_error(msg=msg, lvl='Warning')
            log.warning(msg)

        xlsx_outpath = os.path.join(out_dir, f'{self.name}_report.xlsx')
        log.debug(f'Saving EXCEL results to {xlsx_outpath}')
        wb = xl.Workbook()
        ws = wb.active
        ws.title = 'INFO'
        ws['A1'].value = "Analysis started:"
        ws['A2'].value = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        for k, coll in self.collections.items():
            try:
                pptx_outpath = os.path.join(out_dir, f'{coll.title}_report.pptx')
                with psycopg2.connect(**self.db_params) as con:
                    coll.run_analysis(pg_conn=con,
                                      wb=wb,
                                      pptx_path=pptx_outpath,
                                      pptx_template=pptx_template_path)
            except Exception as e:
                msg = f'Skipped collection {coll.title}: {str(e)}'
                self.add_error(msg=msg)
                log.critical(msg, exc_info=True)
            wb.save(xlsx_outpath)

        wb['INFO']['B2'].value = "Analysis ended:"
        wb['INFO']['B2'].value = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        wb.save(xlsx_outpath)

        log.info(f'END OF ANALYSES for analysis collection {self.name}')
