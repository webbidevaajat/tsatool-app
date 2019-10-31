#!/usr/bin/python
# -*- coding: utf-8 -*-

# Collection of CondCollections

import logging
import os
import psycopg2
import string
import openpyxl as xl
from .cond_collection import CondCollection
from .error import TsaErrCollection
from .utils import trunc_str
from .utils import list_local_statids
from .utils import list_local_sensors
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

    Output files are saved in ``results/`` relative to current working directory.
    PowerPoint reports follow pattern ``results/[name][_sheetname].pptx``,
    and Excel files ``results/[name].xlsx``.
    Existing output files with same filepath will be overwritten.
    """
    def __init__(self, input_xlsx, name):
        self.created_at = datetime.now()
        self.input_xlsx = input_xlsx
        self.name = name
        self.workbook = xl.load_workbook(filename=input_xlsx, read_only=True)

        os.makedirs('results', exist_ok=True)
        self.out_base_path = f'results/{self.name}'

        # This will contain condition rows by Excel sheet,
        # read by a separate method
        self.collections = OrderedDict()

        # Station ids and sensor name-id pairs for dryvalidate checking.
        # These represent the situation as of 8/2019.
        # Updates must be made to corresponding functions in tsa.utils.
        self.local_statids = list_local_statids()
        self.local_sensor_pairs = list_local_sensors()

        # DB connection is made by a separate method only if needed;
        # dryvalidate methods are available also without it.
        self.db_params = DBParams()
        self.db_statids = set()
        self.db_sensor_pairs = dict()

        # Errors are reported on the fly AND collected too
        self.errors = TsaErrCollection('ANALYSIS / EXCEL FILE')

    def add_collections(self, drop=['info']):
        """
        Add CondCollections from worksheets.
        :param drop: list of sheets to exclude by title
        :type drop: list of strings
        """
        sheetnames = [s for s in self.workbook.sheetnames if s.lower().strip() not in drop]
        for title in sheetnames:
            try:
                self.collections[title] = CondCollection.from_xlsx_sheet(
                    ws=self.workbook[title]
                )
                log.info(f'Added CondCollection <{title}>')
            except:
                self.errors.add(msg=f'Could not add CondCollection <{title}>: skipping',
                                log_add='exception')

    def set_sensor_ids(self, pairs):
        """
        Set sensor name-id pairs for all ``Blocks``.

        :param pairs: dict, key = sensor id, value = sensor name
        """
        for coll in self.collections.keys():
            for cnd in self[coll].conditions.keys():
                for bl in self[coll][cnd].blocks.keys():
                    self[coll][cnd][bl].set_sensor_id(pairs)

    def check_statids(self, station_ids):
        """
        For all primary ``Blocks``, check if their station ids are valid,
        i.e., they can be found in ``station_ids`` set.
        ``station_ids`` can be possibly fetched from database.
        """
        # TODO: do this
        pass

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

    def __getitem__(self, key):
        """
        Return the CondCollection from the OrderedDict referenced by ``key``.
        """
        return self.collections[key]

    def __str__(self):
        s = f'<AnalysisCollection {self.name}> from <{self.input_xlsx}> '
        s += f'with {len(self.collections)} collections>'
        return s
