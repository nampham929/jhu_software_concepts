
from app import create_app

# Instantiate the app object from Flask
app = create_app()

'''
Run the app object locally at local host 0.0.0.0
amd port 8080
'''
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
