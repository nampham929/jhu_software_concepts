from flask import Flask

from blueprints.dashboard import dashboard_bp


def create_app():
    app = Flask(__name__)
    app.register_blueprint(dashboard_bp)
    return app


if __name__ == "__main__":
    create_app().run(debug=True)
