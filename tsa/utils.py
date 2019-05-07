# Utility functions for tsa package

import os
import json
import psycopg2

def tsadb_connect(username=None, password=None, ask=False):
    """
    Using ``db_config.json`` file in the root directory,
    connect to the TSA database.
    :param username: database username as string; defaults to
                     'ADMIN_USER' of the config file
    :param password: database user password, only for debugging!
                     Defaults to None -> will be asked
    :param ask:      if ``True``, use interactive input for username;
                     defaults to ``False``
    :return:         connection instance, or None on error
    """
    cf_filename = os.path.join(os.getcwd(), 'db_config.json')
    with open(cf_filename, 'r') as cf_file:
        cf = json.load(cf_file)
    if username is None:
        if ask:
            username = input('Database username: ')
        else:
            username = cf['ADMIN_USER']
    if password is None:
        password = getpass('Password for user "{:s}": '.format(username))
    try:
        pg_conn = psycopg2.connect(dbname=cf['DATABASE'],
                                   user=username,
                                   password=password,
                                   host=cf['HOST'],
                                   port=cf['PORT'],
                                   connect_timeout=5)
        return pg_conn
    except psycopg2.OperationalError as e:
        print('Could not connect to database:')
        print(e)
        print('Are you connected to the right network?')
        print('Using correct and existing username and password?')
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
        errtext += old_x + '\n'
        errtext += '^'
        raise ValueError(errtext)

    if len(x) > 40:
        errtext = 'String too long, maximum is 40 characters:\n'
        errtext += old_x + '\n'
        raise ValueError(errtext)

    for i, c in enumerate(x):
        if not (c.isalnum() or c == '_'):
            errtext = 'String contains an invalid character:\n'
            errtext += old_x + '\n'
            errtext += '~' * i + '^'
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
