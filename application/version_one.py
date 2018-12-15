"""
This is property of ENIGMATICS INC.

VERSION::1
@author::sandy
"""
import os
import sys
from flask import Blueprint
sys.append()

version_one = Blueprint('version_one', __name__)

"""
TOKEN STATUS:
100 : `OK`
200 : `EXPIRED`
"""

@version_one.route('/', methods=('GET'))
def default_route():
    return

@version_one.route('/ping', methods=('GET'))
def api_ping():
    pass

@version_one.route('/token', methods=('GET'))
def get_token():
    pass

@version_one.route('/files', methods=('POST'))
def store_files():
    pass

@version_one.route('/status', methods=('POST'))
def store_status():
    pass

@version_one.route('/releases', methods=('GET'))
def get_releases():
    pass

@version_one.route('/download/filename', methods=('GET'))
def get_file():
    pass
