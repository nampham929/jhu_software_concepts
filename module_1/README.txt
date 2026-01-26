1. Name: Nam Pham - JHED ID: npham21
2. Module Info: Module 1: Personal Website  - Due Date: January 25, 2026
3. Approach:

This assignment is to build a Flask web application that serves a personal website with three pages: Home, Projects, and Contact.

I built and organized the content of each page in the templates folder. I first created a 'base.html' file to set up the overall structure of the website including the navigation bar and the pages. I also linked the 'main.css' file to the 'base.html' file to provide styling for the pages. The 'main.css' file was saved in the 'css' folder. Contents of each page were built and organized in the children templates: 'home.html' file, 'projects.html' file, and 'contact.html' file, which extended the 'base.html' template. The navigation bar is available in each tab, and the current tab is highlighted and colorized in the navigation bar. 

I created the blueprint of the pages in 'routes.py' module. This module renders the contents of each page as users go to each route of the website. The 'routes.py' module was saved in the 'pages' folder, which I turned into a package, so I could later import the blueprint in 'routes.py' module to the '__init__.py' module in the 'app' folder to register the blueprint. In the '__init__.py' module, I created the 'def create_app():' function, which is the application factory. The function would instantiate a Flask object when called.

The 'run.py' module served the function of running the program. I imported the 'def create_app():' function into this module and called it to create a Flask application named 'app'. 'app' is run locally at local host  host 0.0.0.0 and port 8080.


How to run the site:

-Install the required packages in the requirements.txt file
-In VS Code, run 'python run.py'
-In a browser, go to http://127.0.0.1:8080/



4. Known Bugs: 
None

5. Citations:


