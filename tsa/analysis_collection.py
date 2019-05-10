#!/usr/bin/python
# -*- coding: utf-8 -*-

# Collection of CondCollections

import os
import openpyxl as xl
from .cond_collection import CondCollection
from .utils import trunc_str
from datetime import datetime

class Action:
    """
    Template for CLI actions / choices related to AnalysisCollection,
    with some informative attributes.
    """
    def __init__(self, title, content, message=''):
        self.title = title
        self.content = content
        self.message = message

    def __str__(self):
        """
        Pick-compatible string representation, lines max 101 chars
        """
        if not self.message:
            return '{:33} {:67}'.format(trunc_str(self.title, n=33),
                                        trunc_str(self.content, n=67))
        else:
            return '{:33} {:33} {:33}'.format(trunc_str(self.title, n=33),
                                              trunc_str(self.content, n=33),
                                              trunc_str(self.message, n=33))

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
        self.name = name
        # TODO: validate / modify output name
        self.base_dir = os.getcwd()
        self.data_dir = os.path.join(self.base_dir, 'analysis')
        self.sheetnames = []
        self.collections = []
        self.statids_in_db = set()
        self.n_errors = 0
        self.out_formats = ['xlsx', 'pptx']
        # TODO: method for collecting overall errors

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

        # 2nd: validate sheets
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

        # 3rd: list errors / warnings
        ls.append(Action('List errors and warnings',
                         f'{self.n_errors} errors or warnings'))

        # 4th: set output name
        if self.name is None:
            ls.append(Action('Set output name',
                             'No output name set',
                             'Will be auto-generated'))
        else:
            ls.append(Action('Set output name',
                             self.name))

        # 5th: select output formats
        ls.append(Action('Select output formats',
                         ', '.join(self.out_formats)))

        # 6th: run analyses and save output
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

        # 7th: exit program
        ls.append(Action('Exit program', ''))

        return ls

    def set_input_xlsx(self, path):
        """
        Set the input excel file path,
        **relative to** ``[project_root]/analysis/``.
        Throws an error if it does not exist or is not an .xlsx file.
        """
        if not os.path.exists(path):
            raise Exception(f'File {path} does not exist!')
        if not path.endswith('.xslx'):
            raise Exception(f'File {path} is not an .xlsx file!')

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
