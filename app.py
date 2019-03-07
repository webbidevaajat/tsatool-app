#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from flask import Flask
app = Flask(__name__)

@app.route('/')
def main():
    return 'Hello, World!'

app.config['DEBUG'] = True
if __name__ == '__main__':
    app.run()
