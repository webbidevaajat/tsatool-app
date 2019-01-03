#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This module contains tools for operating with road weather station database
using condition strings and aliases.

This module shall be imported by Dash ``app.py``.
"""
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

def make_aliases(raw_cond, master_alias):
    """
    Convert raw condition string into SQL clause of alias blocks
    and detect condition type (primary or secondary).

    Primary condition consists of station#sensor logicals only.
    Secondary condition contains existing primary conditions.

    Master alias must be a valid SQL table name,
    preferably of format letter-number, e.g. "A1".
    Subaliases will be suffixed like _1, _2, ...
    
    :Example:
        >>> make_aliases(raw_cond='(s1122#TIENPINNAN_TILA3 = 8 \
            OR (s1122#KITKA3_LUKU >= 0.30 AND s1122#KITKA3_LUKU < 0.4)) \
            AND s1115#TIE_1 < 2', 
            master_alias='D2')
        {
        'type': 'primary',
        'alias_condition': '(D2_1 OR (D2_2 AND D2_3)) AND D2_4',
        'aliases': {
            'D2_1': {'st': 's1122', 'lgc': 'TIENPINNAN_TILA3 = 8'},
            'D2_2': {'st': 's1122', 'lgc': 'KITKA3_LUKU >= 0.30'},
            'D2_3': {'st': 's1122', 'lgc': 'KITKA3_LUKU < 0.4'}
            'D2_4': {'st': 's1115', 'lgc': 'TIE_1 < 2'}
            }
        }

        >>> make_aliases(raw_cond='D2 AND C33', master_alias='DC')
        {
        'type': 'secondary',
        'alias_condition': 'D2 AND C33'
        'aliases': {
            'D2': {'st': None, 'lgc': 'D2'}
            'C33': {'st': None, 'lgc': 'C33'}
            }
        }
    
    :param raw_cond: raw condition string
    :type raw_cond: string
    :param master_alias: master alias string
    :type master_alias: string
    :return: dict of condition type, alias clause and alias pairs
    :rtype: dict
    :raises: # TODO error type?
    """
    return None
    # TODO write make_aliases()

class Condition:
    """
    Single condition, its aliases, query handling and results.
    
    :Example:
    # TODO example
    
    # TODO Condition init parameters and results
    """
    def __init__(self, raw_condition_string):
        pass
    # TODO write Condition
        
class CondCollection:
    """
    A set of conditions. Main task to prevent duplicates.
    
    # TODO CondCollection init parameters and results
    """
    
    # TODO write CondCollection
    pass