#!/usr/bin/python
# -*- coding: utf-8 -*-

# Errors with level, context and formatting
from collections import OrderedDict

class TsaError():
    """
    Holds an error message with level and context.
    """
    def __init__(self, lvl="", cxt="", msg=""):
        self.level = lvl
        self.context = cxt
        self.message = msg
        self.indent = 0
        self.set_indent()

    def set_indent(self):
        """
        Certain error contexts are pushed more to the left when printed.
        """
        indents = OrderedDict(block = 6,
                              condition = 4,
                              sheet = 2)
        for k, v in indents.items():
            if self.context != '' and k in self.context.lower():
                self.indent = v
                break

    def __str__(self):
        return f'{self.indent * " "}{self.level}, {self.context}: {self.message}'

    def __repr__(self):
        return str(self)

    def __eq__(self, other):
        return self.__dict__ == other.__dict__
