#!/usr/bin/python
# -*- coding: utf-8 -*-

# Condition class, called by CondCollection

import logging
import re
import pandas
import psycopg2
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from .block import Block
from .utils import to_pg_identifier
from .utils import eliminate_umlauts
from .utils import trunc_str
from .tsaerror import TsaError
from matplotlib import rcParams
from datetime import timedelta

log = logging.getLogger(__name__)

# Set matplotlib parameters globally
rcParams['font.family'] = 'sans-serif'
rcParams['font.sans-serif'] = ['Arial', 'Tahoma']

class Condition:
    """
    Single condition, its aliases, query handling and results.

    :example::

        # Making a primary condition:
        >>> Condition('Ylöjärvi_etelään_1',
        ... 'E4',
        ... '(s1122#TIENPINNAN_TILA3 = 8 OR (s1122#KITKA3_LUKU >= 0.30
        ... AND s1122#KITKA3_LUKU < 0.4)) AND s1115#TIE_1 < 2',
        ... ('2018-01-01 00:00', '2018-04-30 23:59'))
        Primary Condition ylojarvi_etelaan_1_e4:
        (s1122#tienpinnan_tila3 = 8 or (s1122#kitka3_luku >= 0.30 and
        s1122#kitka3_luku < 0.4)) and s1115#tie_1 < 2
        ALIAS: (e4_0 or (e4_1 and e4_2)) and e4_3
        >>>
        # Making a secondary condition:
        >>> Condition('Ylöjärvi_pohjoiseen_2',
        ... 'B1',
        ... 'D2 AND D3',
        ... ('2018-01-01 00:00', '2018-04-30 23:59'))
        Secondary Condition ylojarvi_pohjoiseen_2_b1:
        d2 and d3
        ALIAS: b1_0 and b1_1
        >>>
        # Making a secondary condition containing primary blocks:
        >>> Condition('Ylöjärvi_etelään_2',
        ... 'C3',
        ... 'Ylöjärvi_pohjoiseen_2#B1 AND s1011#TIE_1 > 0',
        ... ('2018-01-01 0:00', '2018-04-30 23:59'))
        Secondary Condition ylojarvi_etelaan_2_c3:
        ylojarvi_pohjoiseen_2#b1 and s1011#tie_1 > 0
        ALIAS: c3_0 and c3_1

    :param site: site / location / area identifier
    :type site: string
    :param master_alias: master alias identifier
    :type master_alias: string
    :param raw_condition: condition definition
    :type raw_condition: string
    :param time_range: start (included) and end (included) timestamps
    :type time_range: list or tuple of datetime objects
    :param excel_row: row index referring to the source of the condition in Excel file
    :type excel_row: integer
    """
    def __init__(self, site, master_alias, raw_condition, time_range, excel_row=None):
        # Excel row for prompting, if made from Excel sheet
        self.excel_row = excel_row

        # List for saving errors as they occur
        self.errors = []

        # Original formattings are kept for printing purposes
        self.orig_site = site
        self.orig_master_alias = master_alias
        self.orig_condition = raw_condition

        # Attrs for further use must be PostgreSQL compatible
        self.site = to_pg_identifier(site)
        self.master_alias = to_pg_identifier(master_alias)
        self.id_string = '{:s}_{:s}'.format(self.site, self.master_alias)

        self._condition = eliminate_umlauts(raw_condition.strip().lower())

        # Times must be datetime objects
        self.time_from = time_range[0]
        self.time_until = time_range[1]

        # As above but representing actual min and max time of result data
        self.data_from = None
        self.data_until = None

        # make_blocks() sets .condition_elements, .blocks, .alias_condition and .secondary
        self.condition_elements = []
        self.blocks = []
        self.alias_condition = ''
        self.secondary = None
        self.make_blocks()

        self.station_ids = set()
        self.list_stations()

        self.has_view = False

        # pandas DataFrames for results
        self.main_df = None
        self.durations_df = None

        self.n_rows = 0
        # Total time will be set to represent
        # actual min and max timestamps of the data
        self.tottime = self.time_until - self.time_from
        self.tottime_valid = timedelta(0)
        self.tottime_notvalid = timedelta(0)
        self.tottime_nodata = self.tottime
        self.percentage_valid = 0
        self.percentage_notvalid = 0
        self.percentage_nodata = 1

    @property
    def condition(self):
        return self._condition

    @condition.setter
    def condition(self, raw_condition):
        raw_condition = raw_condition.strip().lower()
        self._condition = eliminate_umlauts(raw_condition)

    def add_error(self, msg, lvl='Error'):
        """
        Add error message to error message list.
        Only unique errors are collected, in order to avoid
        piling up repetitive messages from loops, for example.
        """
        cxt = f'Condition {self.id_string}'
        if self.excel_row is not None:
            cxt += f' (row {self.excel_row} in Excel)'
        err = TsaError(lvl=lvl, cxt=cxt, msg=msg)
        if err not in self.errors:
            self.errors.append(err)

    def make_blocks(self):
        """
        Extract a list of Block instances (that is, subconditions)
        into ``self.blocks`` based on ``self.condition``,
        define ``self.alias_condition`` based on the aliases of the Block instances
        and detect condition type (``secondary == True`` if any of the blocks has
        ``secondary == True``, ``False`` otherwise).
        """
        value = self.condition

        # Generally, opening and closing bracket counts must match
        n_open = value.count('(')
        n_close = value.count(')')
        if n_open != n_close:
            msg = ('Should be as many "(" as ")" characters in the condition, now '
                   f'{n_open} "(" and {n_close} ")"')
            log.error(f'Condition {self.id_string}: {msg}')
            self.add_error(msg)

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
                try:
                    bl = Block(master_alias=self.master_alias,
                        parent_site=self.site,
                        order_nr=i,
                        raw_logic=el)
                    # If a block with same contents already exists,
                    # do not add a new one with another order number i,
                    # but add the existing block with its order number.
                    # The .index() method raises an error in case the tuple with
                    # Block element is NOT contained in the list.
                    existing_blocks = [t for t in idfied if t[0] == 'block']
                    for eb in existing_blocks:
                        if eb[1].raw_logic == bl.raw_logic:
                            idfied.append(eb)
                            break
                    else:
                        idfied.append(('block', bl))
                        i += 1
                except:
                    msg = f'Could not add block on index {i} to condition'
                    log.error(f'Condition {self.id_string}: {msg}', exc_info=True)
                    self.add_error(msg)

        def validate_order(tuples):
            """
            Validate order of the elements of a :py:class:``Block``.

            :param tuples: list of tuples, each of which has
                ``open_par`, ``close_par``, ``andor``, ``not`` or ``block``
                in the first index and the string element itself in the second.
            :type tuples: list or tuple
            :return: no return if element order is valid, otherwise
                raise an error upon first invalid element

            Following element types may be in the first index:
                ``open_par``, ``not``, ``block``

            Following elements may be in the last index:
                ``close_par``, ``block``

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

            errors_in_els = False

            for i, el in enumerate(tuples):
                if i == 0:
                    if el[0] not in allowed_first:
                        msg = f'"{el[1]}" cannot be first element in condition'
                        log.error(f'Condition {self.id_string}: {msg}')
                        self.add_error(msg)
                        errors_in_els = True
                elif i == last_i:
                    if el[0] not in allowed_last:
                        msg = f'"{el[1]}" cannot be last element in condition'
                        log.error(f'Condition {self.id_string}: {msg}')
                        self.add_error(msg)
                        errors_in_els = True
                if i < last_i:
                    if (el[0], tuples[i+1][0]) not in allowed_pairs:
                        msg = f'Cannot have "{el[1]}" before "{tuples[i+1][1]}"'
                        log.error(f'Condition {self.id_string}: {msg}')
                        self.add_error(msg)
                        errors_in_els = True

            return errors_in_els

        # Check the correct order of the tuples.
        # If there are errors, constructing blocks is skipped.
        has_errors = validate_order(idfied)
        if has errors:
            msg = 'There were errors with condition elements, cannot create condition blocks'
            log.error(f'Condition {self.id_string}: {msg}')
            self.add_error(msg)
            return

        # If validation was successful, attributes can be set

        # Pick up all unique blocks in the order they appear
        blocks = []
        for el in idfied:
            if el[0] == 'block' and el[1] not in blocks:
                blocks.append(el[1])
        self.blocks = sorted(blocks, key=lambda x: x.alias)

        # Form the alias condition by constructing the parts back
        # from the "identified" parts, but for blocks, this time
        # use their alias instead of the raw condition string.
        # Whitespaces must be added a bit differently for each type.
        alias_parts = []
        for el in idfied:
            if el[0] == 'andor':
                alias_parts.append(f' {el[1]} ')
            elif el[0] == 'not':
                alias_parts.append(f'{el[1]} ')
            elif el[0] in ('open_par', 'close_par'):
                alias_parts.append(el[1])
            elif el[0] == 'block':
                alias_parts.append(el[1].alias)
        self.alias_condition = ''.join(alias_parts)
        self.condition_elements = idfied

        # If any of the blocks is secondary,
        # then the whole condition is considered secondary.
        self.secondary = False
        for bl in self.blocks:
            if bl.secondary:
                self.secondary = True
                break

    def list_stations(self):
        """
        Add unique station ids of primary ``self.blocks``
        to ``self.station_ids`` set
        """
        for bl in self.blocks:
            if not bl.secondary:
                self.station_ids.add(bl.station_id)

    def create_db_temptable(self, pg_conn=None, verbose=False, src_tables=set(), execute=True):
        """
        Create temporary table corresponding to the condition.
        Existence of secondary block source tables is checked against ``src_tables``.
        Effective only if a database connection is present.
        Views ``statobs_time`` and ``statobs_time`` must exist.
        With execute=False, only returns the sql statements as string.
        """
        log.debug(f'Creating temp table {self.id_string}')
        if len(self.blocks) == 0:
            msg = 'No Blocks to make a database table from, skipping'
            log.error(f'Condition {self.id_string}: {msg}')
            self.add_error(msg)
            return
        # Any relation with the same name is dropped first.
        # An idiot-proof step to prevent base tables from being dropped here
        if self.id_string in ['stations', 'statobs', 'sensors', 'seobs', 'laskennallinen_anturi', 'tiesaa_asema']:
            msg = (f'Cannot use "{self.id_string}" as Condition identifier'
                   'since it is an existing db relation name, skipping')
            log.error(f'Condition {self.id_string}: {msg}')
            self.add_error(msg)
            return
        drop_sql = f"DROP TABLE IF EXISTS {self.id_string};\n"

        # Block-related data structures in the db are defined as temp tables
        # whose lifespan only covers the current transaction:
        # this prevents namespace conflicts with, e.g., similar aliases shared by multiple sites
        # and keeps the identifier reasonably short. Moreover, Block-related
        # datasets are not needed between Conditions (-> db sessions) as such.
        block_defs = []
        for bl in self.blocks:
            s = f"CREATE TEMP TABLE {bl.alias} ON COMMIT DROP AS ({bl.get_sql_def()});"
            block_defs.append(s)

        # Temp table representing the Condition persists along with the connection / session,
        # and it is constructed as follows:
        # - Make the Block parts (dropped at the end of the transaction)
        # - Create the "most granular" validity ranges series from all the Block temp tables as "master_ranges"
        # - Left join the Block temp tables to master_ranges
        # If there is only one Block, master_ranges is not needed.
        create_sql = "\n".join(block_defs)

        if len(self.blocks) == 1:
            create_sql += (f"\nCREATE TEMP TABLE {self.id_string} AS ( \n"
                           "SELECT \n"
                           "lower(valid_r) AS vfrom, \n"
                           "upper(valid_r) AS vuntil, \n"
                           "upper(valid_r)-lower(valid_r) AS vdiff, \n"
                           f"{self.blocks[0].alias}, \n"
                           f"{self.blocks[0].alias} AS master \n"
                           f"FROM {self.blocks[0].alias});")
        else:
            master_seq_els = []
            for bl in self.blocks:
                s = f"SELECT unnest( array [lower(valid_r), upper(valid_r)] ) AS vt FROM {bl.alias}"
                master_seq_els.append(s)
            master_seq_sql = "\nUNION \n".join(master_seq_els)
            create_sql += (f"\nCREATE TEMP TABLE {self.id_string} AS ( \n"
                           "WITH master_seq AS ( \n"
                           f"{master_seq_sql} \n"
                           "ORDER BY vt), \n")
            create_sql += ("master_ranges_wlastnull AS ( \n"
                           "SELECT vt AS vfrom, LEAD(vt, 1) OVER (ORDER BY vt) AS vuntil \n"
                           "FROM master_seq), \n")
            create_sql += ("master_ranges AS ( \n"
                           "SELECT tstzrange(vfrom, vuntil) AS valid_r \n"
                           "FROM master_ranges_wlastnull \n"
                           "WHERE vuntil IS NOT NULL) \n")
            block_join_els = ['master_ranges']
            for bl in self.blocks:
                s = f"LEFT JOIN {bl.alias} ON master_ranges.valid_r && {bl.alias}.valid_r"
                block_join_els.append(s)
            block_join_sql = " \n".join(block_join_els)
            create_sql += ("SELECT \n"
                           "lower(master_ranges.valid_r) AS vfrom, \n"
                           "upper(master_ranges.valid_r) AS vuntil, \n"
                           "upper(master_ranges.valid_r)-lower(master_ranges.valid_r) AS vdiff, \n")
            create_sql +=  ", \n".join([f"{bl.alias}" for bl in self.blocks]) + ", \n"
            create_sql += f"({self.alias_condition}) AS master \nFROM {block_join_sql});"

        if verbose:
            log.info(drop_sql)
            log.info(create_sql)

        if not pg_conn:
            msg = 'No db connection, cannot create db temp table'
            log.error(f'Condition {self.id_string}: {msg}')
            self.add_error(msg)

        if execute and pg_conn:
            with pg_conn.cursor() as cur:
                try:
                    cur.execute(drop_sql)
                    pg_conn.commit()
                    cur.execute(create_sql)
                    pg_conn.commit()
                    self.has_view = True
                except:
                    pg_conn.rollback()
                    msg = 'Could not create db temp table'
                    log.error(f'Condition {self.id_string}: {msg}')
                    self.add_error(msg)

    def set_summary_attrs(self):
        """
        Calculate summary attribute values using the ``.main_df`` DataFrame.
        """
        if self.main_df is None:
            return
        df = self.main_df

        self.n_rows = df.shape[0]
        self.tottime_valid = df[df['master']==True]['vdiff'].sum() or timedelta(0)
        self.tottime_notvalid = df[df['master']==False]['vdiff'].sum() or timedelta(0)
        self.tottime_nodata = self.tottime - self.tottime_valid - self.tottime_notvalid
        tts = self.tottime.total_seconds()
        self.percentage_valid = self.tottime_valid.total_seconds() / tts
        self.percentage_notvalid = self.tottime_notvalid.total_seconds() / tts
        self.percentage_nodata = self.tottime_nodata.total_seconds() / tts

    def fetch_results_from_db(self, pg_conn=None):
        """
        Fetch result data from corresponding db view
        to pandas DataFrame, and set summary attribute values
        based on the DataFrame.
        """
        if not pg_conn:
            msg = 'No database connection, cannot fetch results'
            log.error(f'Condition {self.id_string}: {msg}')
            self.add_error(msg)
            return
        if not self.has_view:
            msg = ('No temp table for this condition in database, '
                   'cannot fetch results')
            log.error(f'Condition {self.id_string}: {msg}')
            self.add_error(msg)
            return
        sql = f"SELECT * FROM {self.id_string};"
        self.main_df = pandas.read_sql(sql, con=pg_conn)

        self.data_from = self.main_df['vfrom'].min()
        self.data_until = self.main_df['vuntil'].max()
        if not (self.data_from is None or self.data_until is None):
            self.tottime = self.data_until - self.data_from

        self.set_summary_attrs()
        log.debug(f'Results fetched and summary attributes set for Condition {self.id_string}')

    def get_timelineplot(self):
        """
        Returns a Matplotlib figure object:
        a `broken_barh` plot of the validity of the condition
        and its blocks on a timeline.
        """
        if self.main_df is None:
            msg = 'Cannot create a plot with no data'
            log.error(f'Condition {self.id_string}: {msg}')
            self.add_error(msg)
            return

        def getfacecolor(val):
            """
            Return a color name
            by boolean column value.
            """
            if val == True:
                return '#f03b20'
            elif val == False:
                return '#2b83ba'
            return '#bababa'

        # Set height and transparency for block rows, between 0-1;
        # master row will be set to height 0.8 and alpha 1 below.
        hgtval = 0.5
        alphaval = 0.5
        # Offset of the logic label above the bar
        lbl_offset = 0.1

        # Make matplotlib-ready range list from the time columns
        xr = zip([mdates.date2num(el) for el in self.main_df['vfrom']],
                 [mdates.date2num(el) for el in self.main_df['vuntil']])
        xr = [(a, b-a) for (a, b) in xr]

        # Make subplots for blocks;
        # for every block, there should be
        # a corresponding boolean column in the result DataFrame!
        fig, ax = plt.subplots()
        yticks = []
        ylabels = []
        i = 1
        for bl in self.blocks:
            logic_lbl = bl.raw_logic
            ax.broken_barh(xranges=xr, yrange=(i, hgtval),
                           facecolors=list(map(getfacecolor,
                                               self.main_df[bl.alias])),
                           alpha=alphaval)
            ax.annotate(s=logic_lbl,
                        xy=(xr[0][0], i + hgtval + lbl_offset))
            yticks.append(i + (hgtval / 2))
            ylabels.append(bl.alias)
            i += 1

        # Add master row to the plot
        hgtval = 0.8
        ax.broken_barh(xranges=xr, yrange=(i, hgtval),
                       facecolors=list(map(getfacecolor,
                                           self.main_df['master'])))
        ax.annotate(s=self.alias_condition,
                    xy=(xr[0][0], i + hgtval + lbl_offset))
        yticks.append(i + (hgtval / 2))
        ylabels.append('master')
        i += 1

        # Set a whole lot of axis parameters...
        ax.set_axisbelow(True)

        ax.xaxis_date()
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%y'))
        ax.xaxis.set_major_locator(mdates.MonthLocator())
        ax.xaxis.set_ticks_position('none')
        ax.xaxis.grid(color='#e5e5e5')
        #plt.xticks(rotation=45)

        ax.set_yticks(yticks)
        ax.set_yticklabels(ylabels)
        ax.yaxis.set_ticks_position('none')

        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['left'].set_visible(False)
        ax.spines['bottom'].set_visible(False)

        return ax

    def save_timelineplot(self, fobj, w, h):
        """
        Save main timeline plot as png picture into given file object
        with given pixel dimensions.
        """
        DPI = 300
        w = w / DPI
        h = h / DPI
        fig = self.get_timelineplot().get_figure()
        fig.dpi = DPI
        fig.set_size_inches(w, h)
        fig.savefig(fname=fobj,
                    format='png')
        plt.close(fig)

    def __getitem__(self, key):
        """
        Returns the Block instance on the corresponding index.
        ``key`` can be integer or ``Block.alias`` string.
        """
        try:
            idx = int(key)
        except ValueError:
            idx = None
            for i, bl in enumerate(self.blocks):
                if bl.alias == key:
                    idx = i
                    break
            if idx is None:
                raise KeyError(f"No Block with alias '{key}'")
        return self.blocks[idx]


    def __str__(self):
        if self.secondary:
            s = '  Secondary '
        else:
            s = '  Primary '
        s += (f'Condition {self.id_string}:\n'
              f'    {trunc_str(self.condition, n=76)}\n'
              f'    ALIAS: {trunc_str(self.alias_condition, n=76)}')
        return s

    def __repr__(self):
        # TODO unambiguous representation?
        return str(self)
