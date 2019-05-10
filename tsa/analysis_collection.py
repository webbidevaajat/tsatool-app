#!/usr/bin/python
# -*- coding: utf-8 -*-

# Collection of CondCollections

import os
import openpyxl as xl
from .cond_collection import CondCollection
from datetime import datetime

class AnalysisCollection:
    """
    A collection of ``CondCollection`` instances.
    Can be used to run multiple analyses as a batch job.
    Enables validating CondCollections separately
    and then analysing them.

    The collection is based on an Excel file,
    and each worksheet produces a CondCollection (unless valid).
    The results (common xlsx file, and a pptx file for each CondCollection)
    are saved into ``output_dir`` and can be optionally zipped.

    .. note: Existing files with same filepath will be overwritten.
    """

    def __init__(self, xlsx_path=None, output_dir=None):
        self.input_xlsx_path = xlsx_path or ''
        self.output_dir = output_dir or ''
        self.title = os.path.basename(output_dir) or ''
        self.collections = []
        self.statids_in_db = set()

    def set_input_xlsx_path(self, path):
        """
        Set the input excel file path.
        Throws an error if it does not exist.
        """
        # TODO
        pass

    def set_output_dir(self, path):
        """
        Set the output directory for result files.
        Will be created if does not exist.
        Throws an error upon invalid path.
        """
        # TODO
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
