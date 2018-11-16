from __future__ import division
from flask import Flask, request, session, g, redirect, url_for, abort, \
     render_template, flash, jsonify, Response
from flask_cors import CORS, cross_origin
import json
import pandas as pd
import os
import sys
import json
import logging


## Database
import psycopg2
import urlparse

def open_connection():
    urlparse.uses_netloc.append("postgres")
    url = urlparse.urlparse(os.environ["DATABASE_URL"])
    connection = psycopg2.connect(
        database=url.path[1:],
        user=url.username,
        password=url.password,
        host=url.hostname,
        port=url.port
        )
    return connection

app = Flask(__name__)
CORS(app)
app.config.from_object(__name__)

app.logger.addHandler(logging.StreamHandler(sys.stdout))
app.logger.setLevel(logging.ERROR)


@app.route('/', methods=['GET', 'POST'])
def index():
    pass

if __name__ == '__main__':
    ## app.run is to run with flask
app.run(debug=True)
