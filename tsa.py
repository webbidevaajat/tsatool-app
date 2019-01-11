#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This module contains tools for operating with road weather station database
using condition strings and aliases.

This module shall be imported by Dash ``app.py``.
"""
import re
import pandas

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
    Converts x (string) such that it can be used as table or column
    identifier in PostgreSQL.
    Raises error if x contains fatally invalid parts, e.g.
    whitespaces or leading digit.

    .. note:: Pg identifier length max is 63 characters.
        To avoid too long final identifiers, max length of x here
        is 40 characters, which should be enough for site names too.
    """
    x = x.strip()

    # Original string without leading/trailing whitespaces
    # is retained for error prompting purposes
    old_x = x
    x = x.lower()
    x = eliminate_umlauts(x)

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
            errtext = 'String contains whitespace or non-alphanumeric character:\n'
            errtext += old_x + '\n'
            errtext += '~' * i + '^'
            raise ValueError(errtext)

    return x

def unpack_logic(raw_logic):
    """
    Makes logic str of format [station]#[sensor] [operator] [value]
    into tuple of these attributes
    and checks validity of the attributes.

    .. note:: Following logical operators are considered:
        '=', '!=', '>', '<', '>=', '<=', 'in'
        'between' is currently not supported.
        If operator is 'in', it is checked whether value after it
        is a valid SQL tuple.
        Operator MUST be surrounded by whitespaces!

    :Example:
        >>> unpack_logic('s1122#KITKA3_LUKU >= 0.30')
        ('s1122', 'kitka3_luku', '>=', '0.30')

    :param raw_logic: original logic string
    :type raw_logic: string
    :returns: station, sensor, operator and value
    :rtype: tuple
    """

    logic_list = raw_logic.split('#')
    if len(logic_list) != 2:
        errtext = 'Too many or no "#"s, should be [station]#[logic]:'
        errtext += raw_logic
        raise ValueError(errtext)

    station = to_pg_identifier(logic_list[0])
    logic = logic_list[1].lower()

    operators = [' = ', ' != ', ' > ', ' < ', ' >= ', ' <= ', ' in ']

    operator_occurrences = 0
    for op in operators:
        if op in logic:
            operator_occurrences += 1
            op_included = op
    if operator_occurrences != 1:
        errtext = 'Too many or no operators, should be one of following with spaces:\n'
        errtext += ','.join(operators) + ':\n'
        errtext += raw_logic
        raise ValueError(errtext)

    logic_parts = logic.split(op_included)
    operator = op_included.strip()
    if len(logic_parts) != 2:
        errtext = 'Too many or missing parts separated by operator "{:s}":\n'.format(operator)
        errtext += raw_logic
        raise ValueError(errtext)

    sensor = to_pg_identifier(logic_parts[0])
    value = logic_parts[1].strip()

    if operator == 'in':
        value_valid = all((
            # Add more criteria if needed
            value.startswith('('),
            value.endswith(')')
            ))
        if not value_valid:
            errtext = 'Value after operator "{:s}" is not a valid tuple:\n'.format(operator)

            errtext += raw_logic
            raise ValueError(errtext)
    else:
        try:
            float(value)
        except ValueError:
            errtext = 'Must be numeric value after "{:s}":\n'.format(operator)
            errtext += raw_logic
            raise ValueError(errtext)

    return (station, sensor, operator, value)

class PrimaryBlock:
    """
    Represents a logical condition of sensor value
    with information of site name and station id.
    This renders as boolean column in temporary db tables.
    For PostgreSQL compatibility, umlauts convert to a and o,
    and all strings are made lowercase.

    :Example:
        >>> PrimaryBlock('D2', 3, 's1122#KITKA3_LUKU >= 0.30')
        {
        'master_alias': 'd2',
        'alias': 'd2_3',
        'station': 's1122',
        'sensor': 'kitka3_luku',
        'operator': '>=',
        'value_str': '0.30',
        }

    # TODO params

    """
    def __init__(self, master_alias, order_nr, raw_condition):
        self.master_alias = to_pg_identifier(master_alias)
        self.alias = self.master_alias + '_' + str(order_nr)

        _lg = unpack_logic(raw_condition)
        self.station = _lg[0]
        self.sensor = _lg[1]
        self.operator = _lg[2]
        self.value_str = _lg[3]

class SecondaryBlock:
    """
    Refers to an existing condition and its site,
    which are used as a block in a secondary condition.

    .. note:: The condition in question should already exist
        in the database. This must be checked at the Condition level.

    :Example:
        >>> SecondaryBlock('A1', 2, 'Ylöjärvi_etelään_2#D2')
        {
        'master_alias': 'a1',
        'alias': 'a1_2',
        'site': 'ylojarvi_etelaan_2',
        'src_alias': 'd2'
        }
    """

    # TODO write SecondaryBlock
    pass

def make_aliases(value, master_alias):
    """
    Convert string value into SQL clause of alias blocks
    and detect condition type (primary or secondary).

    Primary condition consists of station#sensor logicals only.
    Secondary condition contains existing primary conditions.

    Master alias must be a valid SQL table name,
    preferably of format letter-number, e.g. "a1".
    Subaliases will be suffixed like _1, _2, ...

    :Example:
        >>> make_aliases('(s1122#tienpinnan_tila3 = 8 \
            or (s1122#kitka3_luku >= 0.30 and s1122#kitka3_luku < 0.4)) \
            and s1115#tie_1 < 2',
            master_alias='d2')
        {
        'type': 'primary',
        'alias_condition': '(D2_1 OR (D2_2 AND D2_3)) AND D2_4',
        'aliases': {
            'd2_1': {'st': 's1122', 'lgc': 'tienpinnan_tila3 = 8'},
            'd2_2': {'st': 's1122', 'lgc': 'kitka3_luku >= 0.30'},
            'd2_3': {'st': 's1122', 'lgc': 'kitka3_luku < 0.4'}
            'd2_4': {'st': 's1115', 'lgc': 'tie_1 < 2'}
            }
        }

        >>> make_aliases('ylojarvi_etelaan_2#d2 \
        and ylojarvi_etelaan_2#c33', master_alias='dc')
        {
        'type': 'secondary',
        'alias_condition': 'd2 and c33'
        'aliases': {
            'd2': {'st': ylojarvi_etelaan_2, 'lgc': 'd2'}
            'c33': {'st': ylojarvi_etelaan_2, 'lgc': 'c33'}
            }
        }

    :param value: raw condition string
    :type value: string
    :param master_alias: master alias string
    :type master_alias: string
    :return: dict of condition type, alias clause and alias pairs
    :rtype: dict
    :raises: # TODO error type?
    """

    # Generally, opening and closing bracket counts must match
    n_open = value.count('(')
    n_close = value.count(')')
    if n_open != n_close:
        errtext = 'Unequal number of opening and closing parentheses:\n'
        errtext += '{:d} opening and {:d} closing'.format(n_open, n_close)
        raise ValueError(errtext)

    # Eliminate multiple whitespaces
    # and leading and trailing whitespaces
    value = ' '.join(value.split()).strip()

    # Split by
    # - parentheses
    # - and, or, not: must be surrounded by spaces
    # - not: starting the string and followed by space.
    # Then strip results from trailing and leading whitespaces
    # and remove empty elements.
    sp = re.split('([()]|(?<=\s)and(?=\s)|(?<=\s)or(?=\s)|(?<=\s)not(?=\s)|^not(?=\s))', value)
    sp = [el.strip() for el in sp]
    sp = [el for el in sp if el]

    # Handle special case of parentheses after "in":
    # they are part of the logic element.
    # unpack_logic() will detect in the next step
    # if the tuple after "in" is not correctly enclosed by ")".
    new_sp = []
    for el in sp:
        if not new_sp:
            new_sp.append(el)
            continue
        if len(new_sp[-1]) > 3 and new_sp[-1][-3:] == ' in':
            new_sp[-1] = new_sp[-1] + ' ' + el
        elif ' in ' in new_sp[-1] and new_sp[-1][-1] != ')':
            new_sp[-1] = new_sp[-1] + el
        else:
            new_sp.append(el)

    # Identify the "role" of each element by making them into
    # tuples like (role, element).
    # First, mark parentheses and and-or-not operators.
    # The rest should convert to logic blocks;
    # unpack_logic() raises error if this does not succeed.
    idfied = []
    for el in new_sp:
        if el == '(':
            idfied.append(('open_par', el))
        elif el == ')':
            idfied.append(('close_par', el))
        elif el in ['and', 'or']:
            idfied.append(('andor', el))
        elif el == 'not':
            idfied.append(('not', el))
        else:
            idfied.append(('logic', unpack_logic(el)))

    # Validate order of the "roles":
    # - May not start with and or or
    #

class Condition:
    """
    Single condition, its aliases, query handling and results.

    :Example:
    # TODO example

    :param site: site / location / area identifier
    :type site: string
    :param master_alias: master alias identifier
    :type master_alias: string
    :param raw_condition: condition definition
    :type raw_condition: string
    :param time_range: start (included) and end (excluded) timestamps
    :type time_range: list or tuple of strings
    """
    def __init__(self, site, master_alias, raw_condition, time_range):
        # Original formattings are kept for printing purposes
        self.orig_site = site
        self.orig_master_alias = master_alias
        self.orig_condition = raw_condition

        # Attrs for further use must be PostgreSQL compatible
        self.site = to_pg_identifier(site)
        self.master_alias = to_pg_identifier(master_alias)
        self.id_string = '{:s}_{:s}'.format(self.site, self.master_alias)

        # TODO: wrap condition str handling to function or property/setter??
        raw_condition = raw_condition.strip().lower()
        raw_condition = eliminate_umlauts(raw_condition)
        self.condition = raw_condition

        # TODO: convert times to UTC if data uses it?
        self.time_from = time_range[0]
        self.time_until = time_range[1]

        # TODO: alias_condition creation
        self.alias_condition = None

        # TODO: blocks creation
        self.blocks = None

        # TODO: unique occurrences of stations in blocks
        self.stations = set()

        # TODO: type is detected from blocks
        self.type = None

        # TODO: postgres create temp table SQL definition
        self.create_sql = None

        # TODO: pandas DataFrame of postgres temp table contents
        self.data = None

        # TODO: result attrs from self.data
        self.tottime_valid = None
        self.tottime_notvalid = None
        self.tottime_nodata = None
        self.percentage_valid = None
        self.percentage_notvalid = None
        self.percentage_nodata = None

class CondCollection:
    """
    A collection of conditions to analyze.
    All conditions share same analysis time range.

    # TODO CondCollection init parameters and results
    """

    def __init__(self, time_from, time_until, pg_conn=None):
        # TODO: validate time input formats?
        self.time_from = time_from
        self.time_until = time_until
        self.time_range = (self.time_from, self.time_until)

        self.conditions = []
        self.stations = set()
        self.id_strings = set()

        self.pg_conn = pg_conn

    def add_station(self, station):
        """
        Add ``station`` to ``self.stations`` if not already there.
        If Postgres connection is available, create temporary view
        for new station.

        .. note::   station identifier must contain station integer id
                    when letters are removed.
        """
        if station not in self.stations:
            if self.pg_conn:
                st_nr = ''.join(i for i in station if i.isdigit())
                sql = 'CREATE OR REPLACE TEMP VIEW {:s} AS \n'.format(station)
                sql += 'SELECT * FROM observations \n'
                sql += 'WHERE station_id = {:s} \n'.format(st_nr)
                sql += 'AND tstart >= {:s} \n'.format(self.time_from)
                sql += 'AND tend < {:s};'.format(self.time_until)

                # TODO: sql execution, error / warning handling

            self.stations.add(station)

    def add_condition(self, site, master_alias, raw_condition):
        """
        Add new Condition instance, raise error if one exists already
        with same site-master_alias identifier.
        """
        candidate = Condition(site, master_alias, raw_condition, self.time_range)
        if candidate.id_string in self.id_strings:
            errtext = 'Identifier {:s} is already reserved, cannot add\n'.format(candidate.id_string)
            errtext += raw_condition
            raise ValueError(errtext)
        else:
            self.conditions.append(candidate)
            self.id_strings.add(candidate.id_string)
            for s in candidate.stations:
                self.add_station(s)

    @classmethod
    def from_dictlist(cls, dictlist, time_from, time_until, pg_conn=None):
        """
        Create instance and add conditions from list of dicts.
        Dicts must have corresponding keys
        ``'site', 'master_alias', 'raw_condition'``.
        """
        cc = cls(time_from, time_until, pg_conn)
        for d in dictlist:
            cc.add_condition(**d)
        return cc
