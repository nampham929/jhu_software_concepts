from flask import Flask
from app.pages import bp

''' 
The function create_app() will instantiate a Flask
object when call. It's the application factory.
'''
def create_app():
    app = Flask(__name__)
    
    # Register blueprint that controls all pages
    app.register_blueprint(bp)

    return app