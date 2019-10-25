#!/usr/bin/python
# -*- coding: utf-8 -*-

# Collection of CondCollections

import logging
import os
import psycopg2
import openpyxl as xl
from .cond_collection import CondCollection
from .utils import trunc_str
from datetime import datetime
from collections import OrderedDict

DEFAULT_PG_HOST = 'localhost'
DEFAULT_PG_PORT = 5432
DEFAULT_PG_DBNAME = 'tsa'
DEFAULT_PG_USER = 'postgres'
DEFAULT_PG_PASSWORD = 'postgres'

log = logging.getLogger(__name__)

class DBParams:
    """
    Stores parameters for database connection.
    """
    def __init__(self):
        self.dbname = os.getenv('PG_DBNAME', DEFAULT_PG_DBNAME)
        self.user = os.getenv('PG_USER', DEFAULT_PG_USER)
        self.password = os.getenv('PG_PASSWORD', DEFAULT_PG_PASSWORD)
        self.host = os.getenv('PG_HOST', DEFAULT_PG_HOST)
        self.port = os.getenv('PG_PORT', DEFAULT_PG_PORT)

    def __getitem__(self, key):
        return self.__dict__[key]

    def __str__(self):
        s = 'DBParams\n'
        for k in self.keys():
            if k == 'password':
                v = '(not shown)'
            else:
                v = self.__dict__[k]
            s += f'{k:8}: {v}, '
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
    # TODO: Separate dry and db methods.
    def __init__(self, input_xlsx=None, name=None):
        self.input_xlsx = None
        self._name = name or self.autoname()
        self.base_dir = os.getcwd()
        self.data_dir = os.path.join(self.base_dir, 'analysis')
        os.makedirs(self.data_dir, exist_ok=True)
        self.workbook = None
        if input_xlsx:
            self.set_input_xlsx(path=input_xlsx)
        self.sheetnames = []
        self.collections = OrderedDict()
        self.errs = TsaErrCollection('ANALYSIS / EXCEL FILE')
        self.statids_in_db = set()
        self.sensor_pairs = {}
        self.out_formats = ['xlsx', 'pptx', 'log']
        self.db_params = DBParams()

    def set_input_xlsx(self, path):
        """
        Set the input excel file path,
        **relative to** ``[project_root]/analysis/``,
        and read the workbook contents.
        Source excel is obligatory, so an error is raised if not possible to read it.
        """
        try:
            self.workbook = xl.load_workbook(filename=path, read_only=True)
            self.input_xlsx = path
        except:
            self.errs.add(f'Could not read input data from {path}',
                          'exception')
            raise Exception('Quitting due to fatal error')

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
            os.makedirs(outpath, exist_ok=True)
        return outpath

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
        # TODO: Change this to .add_collections(self) that adds all the sheets
        #       except metadata INFO sheet.
        if self.workbook is None:
            raise Exception('No workbook loaded, cannot add collection')
        if title not in self.workbook.sheetnames:
            raise Exception(f'"{title}" not in workbook sheets')

        ws = self.workbook[title]
        # NOTE: sensor pairs input is a bit weird here, could be fixed
        self.collections[title] = CondCollection.from_xlsx_sheet(
            ws,
            station_ids=self.statids_in_db,
            sensor_pairs=self.sensor_pairs)

    def save_sensor_pairs(self, pg_conn=None, pairs=None):
        """
        Get sensor name-id pairs from database or name:id ``pairs`` dict
        and save them as dict.
        ``pg_conn`` must be a valid, online connection instance to TSA db.
        """
        # TODO: Separate dry and db methods.
        if pg_conn is not None:
            with pg_conn.cursor() as cur:
                cur.execute("SELECT id, lower(name) AS name FROM sensors;")
                tb = cur.fetchall()
                self.sensor_pairs =  {k:v for v, k in tb}
        elif pairs is not None:
            self.sensor_pairs = pairs
        else:
            raise Exception("No pg_conn or sensor name-id pairs provided")

    def save_statids_in_statobs(self, pg_conn=None, ids=None):
        """
        Get all station ids that are generally available in observation data,
        or give id list from outside.
        ``pg_conn`` must be a valid, online connection instance to TSA db.
        """
        # TODO: Separate dry and db methods.
        if pg_conn is not None:
            with pg_conn.cursor() as cur:
                cur.execute("SELECT DISTINCT statid FROM statobs ORDER BY statid;")
                statids = cur.fetchall()
                statids = [el[0] for el in statids]
                self.statids_in_db = set(statids)
        elif ids is not None:
            self.statids_in_db = set(ids)
        else:
            raise Exception("No pg_conn or station ids provided to save available station ids")

    def check_statids(self):
        """
        For each CondCollection, check if its station ids
        are available in the ids from the database.
        Returns number of errors occurred.
        """
        # TODO: Separate dry and db methods.
        if not self.statids_in_db:
            err = ('List of available station ids is empty. '
                   'Were they correctly requested from database or set otherwise?')
            raise Exception(err)
        n_errs = 0
        for coll in self.collections.values():
            log.debug(f'Checking station ids for {coll.title} ...')
            if coll.station_ids != self.statids_in_db.intersection(coll.station_ids):
                n_errs += 1
                missing_ids = list(coll.station_ids - self.statids_in_db).sort()
                missing_ids = [str(el) for el in missing_ids]
                err = (f'Following station ids appear in sheet {coll.title} '
                       'but they are NOT available: '
                       ', '.join(missing_ids))
                log.warning(err)
                coll.add_error(err)
        return n_errs

    def dry_validate(self):
        """
        Validate input syntax, ids and sensor names without database,
        using hard-coded station ids and sensor name-id pairs.
        On complete success, return empty string;
        on any error, return error log string.
        """
        # TODO: do this
        pass

    def run_analyses(self):
        """
        Run analyses for CondCollections that were made from the selected Excel sheets,
        and save results according to the selected formats and path names.
        Analyses are run against collection-specific db connections.
        """
        log.info(f'START OF ANALYSES for analysis collection {self.name}')
        wb = None
        ws = None
        pptx_template_path = os.path.join(self.base_dir, 'data', 'report_template.pptx')
        if 'xlsx' in self.out_formats:
            wb_outpath = os.path.join(self.get_outdir(), f'{self.name}_report.xlsx')
            wb = xl.Workbook()
            ws = wb.active
            ws.title = 'INFO'
            ws['A1'].value = f"Analysis started {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            log.info(f'Will save EXCEL results to {wb_outpath}')
        if 'pptx' in self.out_formats:
            if os.path.exists(pptx_template_path):
                log.info(f'Will save POWERPOINT results for each sheet')
            else:
                log.warning((f'Could not find pptx report template file at {pptx_template_path}, '
                            'skipping Powerpoint reports!'))
                pptx_template_path = None
        if 'xlsx' not in self.out_formats and 'pptx' not in self.out_formats:
            log.warning('NO results will be saved')
        for k, coll in self.collections.items():
            try:
                pptx_path = None
                if pptx_template_path is not None:
                    pptx_out_path = os.path.join(self.get_outdir(), f'{coll.title}_report.pptx')
                with psycopg2.connect(**self.db_params) as con:
                    coll.run_analysis(pg_conn=con,
                                      wb=wb,
                                      pptx_path=pptx_out_path,
                                      pptx_template=pptx_template_path)
            except:
                log.critical(f'Skipping collection {coll.title} due to error', exc_info=True)

        if wb is not None:
            ws['A2'].value = f"Analysis ended {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            wb.save(wb_outpath)
            log.info(f'Excel results saved to {wb_outpath}')
        log.info(f'END OF ANALYSES for analysis collection {self.name}')
