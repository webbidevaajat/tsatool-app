#!/usr/bin/python
# -*- coding: utf-8 -*-

# Utility functions for tsa package

import logging
import os
import yaml
import psycopg2
from getpass import getpass

log = logging.getLogger(__name__)

def tsadb_connect(username=None, password=None, ask=False):
    """
    Using ``db_config.yml`` file in the root directory,
    connect to the TSA database.
    :param username: database username as string; defaults to
                     'ADMIN_USER' of the config file
    :param password: database user password, only for debugging!
                     Defaults to None -> will be asked
    :param ask:      if ``True``, use interactive input for username;
                     defaults to ``False``
    :return:         connection instance, or None on error
    """
    with open('db_config.yml', 'r') as f:
        cf = yaml.safe_load(f.read())
    if username is None:
        if ask:
            username = input('Database username: ')
        else:
            username = cf['admin_user']
    if password is None:
        password = os.getenv('POSTGRES_PASSWORD') or getpass('Password for user "{:s}": '.format(username))
    try:
        pg_conn = psycopg2.connect(dbname=cf['database'],
                                   user=username,
                                   password=password,
                                   host=cf['host'],
                                   port=cf['port'],
                                   connect_timeout=5)
        return pg_conn
    except psycopg2.OperationalError as e:
        log.error('Could not connect to tsa database', exc_info=True)
        return None

def eliminate_umlauts(x):
    """
    Converts ä and ö into a and o.
    """
    umlauts = {
        'ä': 'a',
        'Ä': 'A',
        'ö': 'o',
        'Ö': 'O'
    }
    for k in umlauts.keys():
        x = x.replace(k, umlauts[k])

    return x

def with_errpointer(s, pos):
    """
    Print ``s`` + a new line with a pointer at ``pos``th index
    (to show erroneous parts in strings)
    """
    try:
        pos = int(pos)
        s = str(s)
    except ValueError:
        return s
    if pos < 0:
        return s
    return s + '\n' + '~'*pos + '^ HERE'

def to_pg_identifier(x):
    """
    Converts x (string) such that it can be used as a table or column
    identifier in PostgreSQL.

    If there are whitespaces in the middle,
    they are converted into underscores.

    Raises error if x contains fatally invalid parts, e.g.
    leading digit or a non-alphanumeric character.

    .. note:: Pg identifier length maximum is 63 characters.
        To avoid too long final identifiers
        (that might be concatenated from multiple original ones),
        max length of x here
        is 40 characters, which should be enough for site names too.
    """
    x = x.strip()

    # Original string without leading/trailing whitespaces
    # is retained for error prompting purposes
    old_x = x
    x = x.lower()
    x = eliminate_umlauts(x)
    x = x.replace(' ', '_')

    if x[0].isdigit():
        errtext = 'String starts with digit:\n'
        errtext += with_errpointer(x, 0)
        raise ValueError(errtext)

    if len(x) > 40:
        errtext = 'String too long, maximum is 40 characters:\n'
        errtext += with_errpointer(x, 40-1)
        raise ValueError(errtext)

    for i, c in enumerate(x):
        if not (c.isalnum() or c == '_'):
            errtext = 'String contains an invalid character:\n'
            errtext += with_errpointer(x, i)
            raise ValueError(errtext)

    return x

def strfdelta(tdelta, fmt):
    """
    Format timedelta object according to ``fmt`` string.
    ``fmt`` should contain formatting string with placeholders
    ``{days}``, ``{hours}``, ``{minutes}`` and ``{seconds}``.
    """
    d = {'days': tdelta.days}
    d['hours'], rem = divmod(tdelta.seconds, 3600)
    d['minutes'], d['seconds'] = divmod(rem, 60)
    return fmt.format(**d)

def trunc_str(s, n=80):
    """
    Truncate string ``s`` such that ``n-4`` first characters + `` ...``
    are returned (e.g., for printing). For shorter strings,
    return ``s`` as it is.
    """
    if len(s) <= n-4:
        return s
    return s[:(n-5)] + ' ...'
