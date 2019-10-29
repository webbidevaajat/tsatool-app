#!/usr/bin/python
# -*- coding: utf-8 -*-

import logging
from datetime import datetime

log = logging.getLogger(__name__)

class TsaError:
    """
    Store non-fatal errors that can be saved to a log
    or printed after an analysis run, without interrupting the analysis.
    """
    def __init__(self, msg, context, log_add=''):
        self.msg = msg
        self.context = context
        self.timestamp = datetime.now()
        self.n_more = 0
        if log_add == '':
            pass
        elif log_add == 'warning':
            log.warning(self.with_context())
        elif log_add == 'exception':
            log.exception(self.with_context())
        elif log_add == 'fatal':
            log.fatal(self.with_context())
        else:
            log.error(self.with_context())

    def with_context(self):
        """
        Return message with context but no timestamp.
        """
        s = f'{self.context}: {self.msg}'
        if self.n_more > 0:
            s += f' ({self.n_more} more similar errors)'
        return s

    def __str__(self):
        s = f'{self.timestamp}; {self.context}: {self.msg}'
        if self.n_more > 0:
            s += f' ({self.n_more} more similar errors)'
        return s

    def __repr__(self):
        return '<TsaError> ' + str(self)

    def __eq__(self, other):
        return self.msg, self.context == other.msg, other.context

    def __gt__(self, other):
        return self.timestamp > other.timestamp

class TsaErrCollection:
    """
    Container for errors of a tsa object.
    Provides methods for sorting and printing errors.

    :example::

        >>> errs = TsaErrCollection('ANALYSIS / EXCEL FILE')
        >>> errs.add('Could not find Excel file, quitting', log_add='fatal')
    """
    def __init__(self, context):
        self.context = context
        self.errors = list()

    def add(self, msg, log_add=''):
        """
        Add error while preventing duplicate errors;
        for duplicates, only increase the first one's ``.n_more`` for printing.
        """
        e = TsaError(msg, self.context, log_add)
        if e in self.errors:
            self.errors[self.errors.index(e)].n_more += 1
        else:
            self.errors.append(e)

    def short_str(self):
        """
        Collect error messages to one line in time order.
        """
        errs = [e.msg for e in sorted(self.errors)]
        return '; '.join(errs)

    def __len__(self):
        return len(self.errors)

    def __str__(self):
        return '\n'.join([str(e) for e in sorted(self.errors)])

    def __repr__(self):
        return f'<TsaErrCollection> with {len(self)} errors'
