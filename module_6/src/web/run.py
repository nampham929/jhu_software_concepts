"""Flask entrypoint for the module_6 web service."""

from app.flask_app import create_app


if __name__ == "__main__":
    create_app().run(host="0.0.0.0", port=8080)
