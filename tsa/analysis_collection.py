#!/usr/bin/python
# -*- coding: utf-8 -*-

# Collection of CondCollections

import os
import json
import openpyxl as xl
from .cond_collection import CondCollection
from .utils import trunc_str
from datetime import datetime
from getpass import getpass

class DBParams:
    """
    Stores parameters for database connection.
    """
    def __init__(self, dbname=None, user=None, host=None, port=5432):
        self.dbname = dbname
        self.user = user
        self.password = None    # WARNING: stored as plain str
        self.host = host
        self.port = port

    def read_config(self, path):
        """
        Read and set params from file (except password)
        """
        with open(path, 'r') as cf_file:
            cf = json.load(cf_file)
        self.dbname = cf['DATABASE']
        self.user = cf['ADMIN_USER']
        self.host = cf['HOST']
        self.port = cf['PORT']

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

    Any input and output of an analysis is / must be located
    in ``analysis`` directory of the project root.
    The collection is based on an Excel file,
    and each worksheet produces a CondCollection (unless valid).
    The results (common xlsx file, and a pptx file for each CondCollection)
    are saved into a new directory named after ``name``,
    or after the Excel filename if no name given,
    and they can be optionally zipped.

    .. note: Existing files with same filepath will be overwritten.
    """

    def __init__(self, input_xlsx=None, name=None):
        self.input_xlsx = input_xlsx
        # TODO: validate / modify filename
        self.name = name or self.autoname()
        # TODO: validate / modify output name
        self.base_dir = os.getcwd()
        self.data_dir = os.path.join(self.base_dir, 'analysis')
        self.workbook = None
        self.sheetnames = []
        self.collections = []
        self.statids_in_db = set()
        self.n_errors = 0
        self.out_formats = ['xlsx', 'pptx']
        self.db_params = DBParams()
        # TODO: method for collecting overall errors

    def set_input_xlsx(self, path):
        """
        Set the input excel file path,
        **relative to** ``[project_root]/analysis/``,
        and read the workbook contents.
        Throws an error if it does not exist or is not an .xlsx file.
        """
        if not os.path.exists(path):
            raise Exception(f'File {path} does not exist!')
        if not path.endswith('.xlsx'):
            raise Exception(f'File {path} is not an .xlsx file!')
        self.input_xlsx = path
        self.workbook = xl.load_workbook(filename=path,
                                         read_only=True)

    def set_sheetnames(self, sheets):
        """
        Set Excel sheets to analyze.
        """
        if self.workbook is None:
            raise Exception('No Excel workbook selected!')
        self.sheetnames = []
        for s in sheets:
            if s not in self.workbook.sheetnames:
                raise Exception(f'"{s}" is not in workbook sheets!')
            self.sheetnames.append(s)

    @staticmethod
    def autoname():
        """
        Autogenerate a name by timestamp
        """
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        return f'analysis_{ts}'

    def set_output_dir(self, path):
        """
        Set the output directory for result files,
        **relative to** ``[project_root]/analysis/``.
        Will be created if does not exist.
        Throws an error upon invalid path.
        """
        pass

    def add_collection(self):
        """
        Add a CondCollection and ensure it has a ``title``.
        Duplicate titles are not allowed.
        """
        # TODO
        pass

    def read_collections(self):
        """
        Read in the CondCollections from Excel file worksheets.
        Record errors for any invalid sheet that was omitted.
        """
        # TODO
        pass

    def get_collection_titles(self):
        """
        Return a list of titles of CondCollections available.
        """
        return [coll.title for coll in self.collections]

    def save_statids_in_statobs(self, pg_conn):
        """
        Get all station ids that are generally available in observation data.
        ``pg_conn`` must be a valid, online connection instance to TSA db.
        """
        with pg_conn.cursor() as cur:
            sql = "SELECT DISTINCT statid FROM statobs ORDER BY statid;"
            cur.execute(sql)
            statids = cur.fetchall()
            statids = [el[0] for el in statids]
            self.statids_in_db = set(statids)

    def check_statids(self):
        """
        For each CondCollection, check if its station ids
        are available in the ids from the database.
        """
        if not self.statids_in_db:
            err = ('List of available station ids in db is empty.\n'
                   'Were they correctly requested from database?')
            raise Exception(err)
        for coll in self.collections:
            if coll.station_ids != self.statids_in_db.intersection(coll.station_ids):
                missing_ids = list(coll.station_ids - self.statids_in_db).sort()
                missing_ids = [str(el) for el in missing_ids]
                err = ('WARNING: Following station ids are not available in db observations:'
                       ', '.join(missing_ids))
                coll.add_error(err)


    def run_analyses(self, indices=None):
        """
        Run analyses for CondCollections selected by given int indices,
        or for all collections if none given.
        This means everything that is run against collection-specific
        database connections.
        """

    def __getitem__(self):
        """
        Return a CondCollection by list-style int index
        or by dict-style index based on its title.
        """
        # TODO
        pass
