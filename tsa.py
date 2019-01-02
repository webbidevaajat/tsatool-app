#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This module contains tools for operating with road weather station database
using condition strings and aliases.

This module shall be imported by Dash ``app.py``.
"""
import pandas

def make_aliases(raw_cond):
    """
    Convert raw condition string into alias blocks.
    
    :Example:
    # TODO make_aliases() example
    
    :param raw_cond: raw condition string
    :type raw_cond: string
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
