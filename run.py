from flask import Flask
from application.version_one import version_one, one_limiter, jwt_manager

app = Flask(__name__)
one_limiter.init_app(app)
jwt_manager.init_app(app)

app.register_blueprint(version_one, url_prefix='/v1')

if __name__ == "__main__":
    app.run()
