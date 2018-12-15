from flask import Flask
from application.version_one import version_one

app = Flask(__name__)

app.register_blueprint(version_one, url_prefix='/v1')

if __name__ == "__main__":
    app.run()