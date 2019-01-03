#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This module contains tools for operating with road weather station database
using condition strings and aliases.

This module shall be imported by Dash ``app.py``.
"""
import pandas

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
            'D2_2': {'st': 's1115', 'lgc': 'TIE_1 < 2'}
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
        
    # TODO write Condition
        
class CondCollection:
    """
    A set of conditions. Main task to prevent duplicates.
    
    # TODO CondCollection init parameters and results
    """
    
    # TODO write CondCollection
