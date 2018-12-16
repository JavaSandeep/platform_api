"""
This is property of ENIGMATICS INC.

VERSION::1
@author::Sandeep Dudhraj
@company::ENIGMATICS
@source::python3

`This is version of API that would be consumeable by our ROVER ADAPTER.
It would be used to push data to Cloud.
Get upgrades for rover.
Request are authenticate using tokens.
Tokens will be renewed everyday.`
"""
import os
import sys
import json
from flask import Blueprint
from flask import request
from flask_limiter import Limiter
sys.append(os.path.join(os.environ('PLATFORM_HOME'), "utils", "apiutils"))
from apiutils import APIUtils

def get_rate_limits():
    try:
        config_file_path = os.path.join(os.environ('PLATFORM_HOME'), "config", "config.json")
        with open(config_file_path) as f:
            config = json.load(f)
    except Exception as ex:
        print("Could not get configurations. {0}".str(ex))
        sys.exit(-1)
        
    return config.get('api').get('rate-limits')


def get_user_key():
    return request.headers.get('user-key')

version_one = Blueprint('version_one', __name__)
one_limiter = Limiter(
    key_func = get_user_key,
    RATELIMIT_STRATEGY = 'fixed-window-elastic-expiry',
    default_limits=["500 per 1 day"]
)
rate_limits = get_rate_limits()

"""
TOKEN STATUS:
100 : `OK`
200 : `EXPIRED`
"""

@version_one.route('/', methods=('GET'))
@APIUtils.authenticate()
@one_limiter.limit()
def default_route():
    return

@version_one.route('/ping', methods=('GET'))
@APIUtils.authenticate()
@one_limiter.limit()
def api_ping():
    pass

@version_one.route('/token', methods=('GET'))
@APIUtils.authenticate()
def get_token():
    pass

@version_one.route('/files', methods=('POST'))
@APIUtils.authenticate()
@one_limiter.limit()
@APIUtils.validate_size
def put_files():
    pass

@version_one.route('/status', methods=('POST'))
@APIUtils.authenticate()
@one_limiter.limit(rate_limits['data-limit'])
@APIUtils.validate_size
def put_status():
    pass

@version_one.route('/releases', methods=('GET'))
@APIUtils.authenticate()
@one_limiter.limit()
def get_releases():
    pass

@version_one.route('/download/filename', methods=('GET'))
@APIUtils.authenticate()
@one_limiter.limit(rate_limits['download-limit'])
def get_file():
    pass
