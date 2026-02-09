1. Name: Nam Pham - JHED ID: npham21
2. Module Info: Module 3: Database Queries Assignment Experiment - Due Date: February 8, 2026
3. Approach:

This assignment is to build a PostgreSQL database, query the data to answer questions regarding different aspects of the student admission into different schools. and display the results on a Flask website, which allows data updating with buttons.

The first module that I built is load_data.py. The module first connects to the PostgreSQL database by the "def create_connection(db_name, db_user, db_password, db_host, db_port):" function. It then create the data table in the database with the "def create_applicants_table(connection):" function. The columns of the table include: p_id, program, comments, date_added, url, status, term, us_or_international, gpa, gre, gre_v, gre_aw, degree, llm_generated_program, llm_generated_university. The "def parse_date(date_string):" function converted date string in format 'Month DD, YYYY' to 'YYYY-MM-DD'.The "def detect_file_encoding(file_path):" function determines the right type of encoding used to open the JSONL file. The "def load_data_from_jsonl(connection, jsonl_file):" function loads data from JSONL file into the aplicants table. It skips any empty line if exists.

The second module is query_data.py. This module query the data in the PostgreSQL database to answer the assignment questions. The module starts with the "def get_queries():" function which returns a list of queries needed to run on the applicants table, with metadata for display formatting. It then creates a connection to PostgreSQL database by the function "def create_connection(db_name, db_user, db_password, db_host, db_port):". The "def execute_query(connection, sql_query, params=None):" will take in the SQL queries and return raw SQL data in response to those queries. The "def format_display(rows, display_mode, display_labels=None):" and the "def run_query():" functions handle formatting and displaying the results on the console in a user-friendly manner.

The third module is flask_app.py, which hosts the Flask website that displays the results of the queries. It creates the Flask application and register the dashboard blueprint from dashboard.py.

The fourth module is dashboard.py. This module handles the materials displayed on the website and also the buttons to scrape new data, update the database, and refresh the website with updated database. This is an I/O‑heavy task. Most time is spent waiting on network requests and DB I/O. The thread lets the long data‑pull run in the background while the main Flask request thread handles normal HTTP requests and provides updates on the website about the data pull status. The "def create_connection(db_name, db_user, db_password, db_host, db_port):" creates the connection with the PostgreSQL database. The "def load_query_results():" function loads all queries from query_data.py and prepare results for rendering. The "def _ensure_module_2_on_path() -> str:" function turns the module_2 folder into a package, so that the program can use the modules inside the folder for data scraping and cleaning. The "def _fetch_existing_urls(connection) -> set:" function gets a set of URLs already in the DB, so the program can skip duplicates when pulling new data. The pull job checks new entries against that set to skip duplicates. The "def _fetch_latest_url(connection) -> str | None:" function returns the most recent applicant URL in the database, so the pull job can stop when it reaches that URL to avoid duplicates. The :def pull_gradcafe_data(progress_callback=None) -> dict:" function used scrape.py and clean.py to scrape and clean the new data. The new data is stored in "new_data.json" for easy inspection, and the new data is also inserted into the database "applicants" table. The "def _get_pull_in_progress() -> bool:", "def _set_pull_in_progress(value: bool) -> None:", "def _try_start_pull() -> bool:", "def _update_pull_status(message=None, progress=None) -> None:", and "def _get_pull_status_snapshot() -> dict:" functions dictate how the "Pull Data" process and the "Update Analysis" process run and also provide the status of the processes on the website. The program then renders the main dashboard page with live query results and status of the data pull process as the "Pull Data" button is clicked. The "def _run_pull_job() -> None:" carries out the actual data pulling work. The "def update_analysis():" is the logic behind the Update Analysis” button. It decides whether the button becomes available for users and what message to show after they click it. The actual refresh happens when the page reloads and queries run again.

The "dashboard.html: file is the Flask template that defines the dashboard’s structure and content (buttons, query cards, status panel, and the polling script). It renders server data into the page using Jinja.

The "dashboard.css: file is the stylesheet that controls how the dashboard looks (layout, colors, typography, spacing, and responsiveness).


How to run the program:

-Install the required packages in the requirements.txt file
-In VS Code, run 'python load_data.py'to load data to the PostgreSQL database
-In VS Code, run 'python query_data.py' to query data for the questions
-In VS Code, run 'python flask_app.py' to run the website.

The SSH URL to my GitHub repository:
git@github.com:nampham929/jhu_software_concepts.git

4. Known Bugs: 


5. Citations:



