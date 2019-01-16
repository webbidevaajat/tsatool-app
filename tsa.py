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

class Block:
    """
    Represents a logical subcondition
    with information of site name and station id.
    See :py:class:`Condition` that consists of blocks
    and operators and parentheses.
    A Block renders as boolean column in temporary db tables.
    For PostgreSQL compatibility, umlauts convert to a and o,
    and all strings are made lowercase.

    For a *primary* block, the `raw_condition` must consist of
    a station identifier, hashtag, sensor identifier,
    operator and a value.
    For a *secondary* block, the `raw_condition` must consist of
    a site identifier, hashtag and alias identifier. Note that these should
    refer to an existing Condition instance.

    :Example:
        # Making a primary block:
        >>> Block('d2', 'ylojarvi_etelaan_2', 3, 's1122#kitka3_luku >= 0.30')
        {'raw_logic': 's1122#kitka3_luku >= 0.30',
        'master_alias': 'd2',
        'parent_site': 'ylojarvi_etelaan_2',
        'alias': 'd2_3',
        'secondary': False,
        'site': 'ylojarvi_etelaan_2',
        'station': 's1122',
        'source_alias': None,
        'sensor': 'kitka3_luku',
        'operator': '>=',
        'value_str': '0.30'}
        # Making a secondary block:
        >>> Block('d2', 4, 'ylojarvi_pohjoiseen_1#c3')
        {'ylojarvi_pohjoiseen_1#c3',
        'master_alias': 'd2',
        'parent_site': 'ylojarvi_etelaan_2',
        'alias': 'd2_4',
        'secondary': True,
        'site': 'ylojarvi_pohjoiseen_1',
        'station': None,
        'source_alias': 'c3',
        'sensor': None,
        'operator': None,
        'value_str': None}

    :param master_alias: master alias identifier of the parent condition
    :type master_alias: string
    :param parent_site: site identifier of the parent condition
    :type parent_site: string
    :param order_nr: index of the block within the parent condition
    :type order_nr: integer
    :param raw_logic: logic to parse, bound to single sensor or existing Condition
    :type raw_logic: string
    """
    def __init__(self, master_alias, parent_site, order_nr, raw_logic):
        self.raw_logic = raw_logic
        self.master_alias = to_pg_identifier(master_alias)
        self.parent_site = to_pg_identifier(parent_site)
        self.alias = self.master_alias + '_' + str(order_nr)
        self.secondary = None
        self.site = None
        self.station = None
        self.source_alias = None
        self.sensor = None
        self.operator = None
        self.value_str = None

        # Set values depending on raw logic given
        self.unpack_logic()

    def unpack_logic(self):
        """
        Detects and sets block type and attributes from raw logic string
        and checks validity of the attributes.

        .. note:: Following binary operators are considered:
            '=', '!=', '>', '<', '>=', '<=', 'in'
            'between' is currently not supported.
            If operator is 'in', it is checked whether value after it
            is a valid SQL tuple.
            Operator MUST be surrounded by whitespaces!

        :param raw_logic: original logic string
        :type raw_logic: string
        """
        binops = [' = ', ' != ', ' > ', ' < ', ' >= ', ' <= ', ' in ']

        # ERROR if too many hashtags or operators
        n_hashtags = self.raw_logic.count('#')
        if n_hashtags > 1:
            errtext = 'Too many "#"s, only one or zero allowed:\n'
            errtext += self.raw_logic
            raise ValueError(errtext)
        n_binops = 0
        binop_in_str = None
        for binop in binops:
            if binop in self.raw_logic:
                n_binops += self.raw_logic.count(binop)
                binop_in_str = binop
        if n_binops > 1:
            errtext = 'Too many binary operators, only one or zero allowed:\n'
            errtext += self.raw_logic

        # Case 1: contains no hashtag and no binary operator
        # -> secondary block, site is picked from parent_site.
        # Must NOT contain binary operator.
        if n_hashtags == 0 and n_binops == 0:
            self.secondary = True
            self.site = self.parent_site
            self.source_alias = to_pg_identifier(self.raw_logic)

        # Case 2: contains hashtag but no binary operator
        # -> secondary block
        elif n_hashtags == 1 and n_binops == 0:
            self.secondary = True
            parts = self.raw_logic.split('#')
            self.site = to_pg_identifier(parts[0])
            self.source_alias = to_pg_identifier(parts[1])

        # Case 3: contains hashtag and binary operator
        # -> primary block
        elif n_hashtags == 1 and n_binops == 1:
            self.secondary = False
            self.site = self.parent_site
            parts = self.raw_logic.split('#')
            parts = [parts[0]] + parts[1].split(binop_in_str)
            self.station = to_pg_identifier(parts[0])
            self.sensor = to_pg_identifier(parts[1])
            self.operator = binop_in_str.lower().strip()
            self.value_str = parts[2].lower().strip()

            # Special case with operator "in":
            # must be followed by tuple enclosed with parentheses.
            if self.operator == 'in':
                val_sw = self.value_str.startswith('(')
                val_ew = self.value_str.endswith(')')
                if val_sw is False and val_ew is False:
                    errtext = 'Binary operator "in" must be followed by\n'
                    errtext += 'a tuple enclosed with parentheses "()":\n'
                    errtext += self.raw_logic
                    raise ValueError(errtext)

        # Case 4: ERROR if binary operator but no hashtag
        else:
            errtext = 'No "#" given, should be of format\n'
            errtext += '[station]#[sensor] [binary operator] [value]:\n'
            errtext += self.raw_logic
            raise ValueError(errtext)


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

        # TODO: use datetime objects?
        self.time_from = time_range[0]
        self.time_until = time_range[1]

        self.blocks = self.make_blocks()

        # TODO: alias_condition creation
        self.alias_condition = None

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

    def make_blocks(self):
        # TODO: alias condition str is made too
        """
        Extract a list of Block instances (that is, subconditions)
        into `self.blocks` based on `self.condition`
        and detect condition type (secondary if any of the blocks has
        `secondary == True`, primary otherwise).
        """
        value = self.condition

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
        # Block() will detect in the next step
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
        # Block() raises error if this does not succeed.
        idfied = []
        i = 0
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
                idfied.append(('block',
                    Block(master_alias=self.master_alias,
                        parent_site=self.site,
                        order_nr=i,
                        raw_logic=el)))
                i += 1

        def validate_order(tuples):
            """
            Validate order of the elements of a :py:class:`Block`.

            :param tuples: list of tuples, each of which has
                `open_par`, `close_par`, `andor`, `not` or `block`
                in the first index and the string element itself in the second.
            :type tuples: list or tuple
            :return: no return if element order is valid, otherwise
                raise an error upon first invalid element

            Following element types may be in the first index:
                `open_par`, `not`, `block`

            Following elements may be in the last index:
                `close_par`, `block`

            For elements other than the last one, see the table below
            to see what element can follow each element.
            Take the first element from left and the next element from top.

            +-------------+------------+-------------+---------+-------+---------+
            |             | `open_par` | `close_par` | `andor` | `not` | `block` |
            +=============+============+=============+=========+=======+=========+
            | `open_par`  | OK         | X           | X       | OK    | OK      |
            +-------------+------------+-------------+---------+-------+---------+
            | `close_par` | X          | OK          | OK      | X     | X       |
            +-------------+------------+-------------+---------+-------+---------+
            | `andor`     | OK         | X           | X       | OK    | OK      |
            +-------------+------------+-------------+---------+-------+---------+
            | `not`       | OK         | X           | X       | X     | OK      |
            +-------------+------------+-------------+---------+-------+---------+
            | `block`     | X          | OK          | OK      | X     | X       |
            +-------------+------------+-------------+---------+-------+---------+
            """
            allowed_first = ('open_par', 'not', 'block')
            allowed_pairs = (
            ('open_par', 'open_par'), ('open_par', 'not'), ('open_par', 'block'),
            ('close_par', 'close_par'), ('close_par', 'andor'),
            ('andor', 'open_par'), ('andor', 'not'), ('andor', 'block'),
            ('not', 'open_par'), ('not', 'block'),
            ('block', 'close_par'), ('block', 'andor')
            )
            allowed_last = ('close_par', 'block')
            last_i = len(tuples) - 1

            for i, el in enumerate(tuples):
                if i == 0:
                    if el[0] not in allowed_first:
                        errtext = '{"{:s}" not allowed as first element:\n'.format(el[1])
                        raise ValueError(errtext)
                elif i == last_i:
                    if el[0] not in allowed_last:
                        errtext = '"{:s}" not allowed as last element:\n'.format(el[1])
                        raise ValueError(errtext)
                if i < last_i:
                    if (el[0], tuples[i+1][0]) not in allowed_pairs:
                        errtext = '"{:s}" not allowed right before "{:s}":\n'.format(el[1], tuples[i+1][1])
                        raise ValueError(errtext)

        # Check the correct order of the tuples.
        # This should raise and error and thus exit the script
        # if there is an illegal combination of elements next to each other.
        validate_order(idfied)

        # TODO TEST + return blocks, handle alias condition string?


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
