#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from flask_wtf import FlaskForm
from flask_wtf.file import FileField
from flask_wtf.file import FileRequired
from flask_wtf.file import FileAllowed
from wtforms import SubmitField

class UploadForm(FlaskForm):
    file = FileField('Upload an Excel file with conditions to analyze',
                         validators=[
                             FileRequired(),
                             FileAllowed(['xlsx'], 'Excel files (.xlsx)')
                             ])
    submit = SubmitField()

class SelectSheetForm(FlaskForm):
    pass
