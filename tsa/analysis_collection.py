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
from collections import OrderedDict

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
        self.input_xlsx = None
        # TODO: validate / modify filename
        self._name = self.name = name or self.autoname()
        # TODO: validate / modify output name
        self.base_dir = os.getcwd()
        self.data_dir = os.path.join(self.base_dir, 'analysis')
        assert os.path.exists(self.data_dir)
        self.workbook = None
        if input_xlsx:
            self.set_input_xlsx(path=input_xlsx)
        self.sheetnames = []
        self.collections = OrderedDict()
        self.errmsgs = []
        self.statids_in_db = set()
        self.sensor_pairs = {}
        self.out_formats = ['xlsx', 'pptx', 'log']
        self.db_params = DBParams()

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

    @staticmethod
    def autoname():
        """
        Autogenerate a name by timestamp
        """
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        return f'analysis_{ts}'

    def get_outdir(self):
        """
        Return output directory path based on ``data_dir``
        and ``name``. Make if it does not exist.
        """
        outpath = os.path.join(self.data_dir, self.name)
        if not os.path.exists(outpath):
            os.mkdir(outpath)
        return outpath

    def add_error(self, e):
        """
        Add error message to error message list.
        Only unique errors are collected, in order to avoid
        piling up repetitive messages from loops, for example.
        """
        if e not in self.errmsgs:
            self.errmsgs.append(e)

    def list_errors(self):
        """
        Collect together all error and warning messages
        of condition collections and their child items,
        return as list of strings.
        """
        errs = [f'SESSION: {m}' for m in self.errmsgs]
        for condcoll in self.collections.values():
            msgs = [f'COLLECTION: {m}' for m in condcoll.errmsgs]
            errs.extend(msgs)
            for cond in condcoll.conditions:
                msgs = [f'CONDITION: {m}' for m in cond.errmsgs]
                errs.extend(msgs)
        return errs

    def add_collection(self, title):
        """
        Add a CondCollection from worksheet by title.
        This does *not* account for ``.sheetnames``: that is only a container
        for further tools that add pre-selected worksheets
        Duplicate titles are not allowed.
        If sensor name-id pair dictionary is available,
        it can be given here for successful construction of Blocks.

        .. note: Adding by an existing title overwrites the old collection.
        """
        if self.workbook is None:
            raise Exception('No workbook loaded, cannot add collection')
        if title not in self.workbook.sheetnames:
            raise Exception(f'"{title}" not in workbook sheets')

        ws = self.workbook[title]
        # NOTE: sensor pairs input is a bit weird here, could be fixed
        self.collections[title] = CondCollection.from_xlsx_sheet(ws,
            sensor_pairs=self.sensor_pairs)

    def save_sensor_pairs(self, pg_conn):
        """
        Get sensor name-id pairs from database and save them as dict.
        ``pg_conn`` must be a valid, online connection instance to TSA db.
        """
        with pg_conn.cursor() as cur:
            cur.execute("SELECT id, lower(name) AS name FROM sensors;")
            tb = cur.fetchall()
            self.sensor_pairs =  {k:v for v, k in tb}

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
        Returns number of errors occurred.
        """
        if not self.statids_in_db:
            err = ('List of available station ids in db is empty.\n'
                   'Were they correctly requested from database?')
            raise Exception(err)
        n_errs = 0
        for coll in self.collections.values():
            print(f'Checking station ids for "{coll.title}" ...')
            if coll.station_ids != self.statids_in_db.intersection(coll.station_ids):
                n_errs += 1
                missing_ids = list(coll.station_ids - self.statids_in_db).sort()
                missing_ids = [str(el) for el in missing_ids]
                err = ('WARNING: Following station ids are not available in db observations:\n'
                       ', '.join(missing_ids))
                print(err)
                coll.add_error(err)
        return n_errs


    def run_analyses(self, indices=None):
        """
        Run analyses for CondCollections selected by given int indices,
        or for all collections if none given.
        Analyses are run against collection-specific db connections.
        """
        pass

    def __getitem__(self):
        """
        Return a CondCollection by list-style int index
        or by dict-style index based on its title.
        """
        # TODO
        pass
