from flask import Flask

''' 
The function create_app() will instantiate a Flask
object when call. 
'''
def create_app():
    app = Flask(__name__)
    return app