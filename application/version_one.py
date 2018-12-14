from flask import Blueprint

version_one = Blueprint('version_one', __name__, url_prefix='/v1')

@version_one.route('/', methods=('GET'))
def default_route():
    return

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
