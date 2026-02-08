1. Name: Nam Pham - JHED ID: npham21
2. Module Info: Module 2: Web Scraping - Due Date: February 1, 2026
3. Approach:

This assignment is to build a web scraper to scrape data from gradcafe.com. Scraped data is then parsed. Needed information is collected and stored in JSON format files.

I first built the scrape.py module. The module first check the robots.txt file of GradCafe.com using the function 'def _check_robots_allowed(base_url: str, user_agent: str = "Mozilla/5.0") -> str:' to ensure that we are allowed to access their data. This module extracts the HTML code from the website gradcafe.com using the "def _fetch_html(url: str) -> str:" function and convert the bytecodes to readable text. I then use mainly Beautifulsoup methods to extract the needed information and store the information in dictionaries. This is done by the function "def _parse_page(html: str) -> list:". All the needed student application information is in the "tbody" section, so I capture that first. I then extracted the rows from the table body and iterated through each row using the "while loop". In each row, the application data is stored between "td" tags. All data in each row is then collected and stored as a list named "cols". Beautifulsoup methods and regex methods are performed on the list elements to obtain the needed information. Each record has information in a pair of rows. Same approach applied to each pair of rows to extract information. Application information from each record is stored in individual dictionary. All dictionaries are stored in a list named "results". The "def scrape_data(pages: int = ) -> list:" function loops through the pages. The output is stored as a JSON file named 'applicant_data.json'.

The second module of the program is clean.py. This module cleans and standardizes applicant data stored in 'applicant_data.json'. It first loads the file using the 'def load_data(filename: str = "applicant_data.json") -> list:' function to obtain a list of dictionaries. The 'def clean_data(data: list) -> list:' function uses a 'for loop' to go through each dictionary and remove the HTML tags, replace multiple spaces, tabs, or line breaks with a single space, and replace placeholder junk values with empty string. The cleaned file is saved as 'cleaned_applicant_data.json' by the 'def save_data(data: list, filename: str = "cleaned_applicant_data.json") -> None:' function. 

The last module is 'run.py'. This is basically where I run the program. I imported the 'scrape.py' module and 'clean.py' module in this module. I created a 'def main():' function to call the functions from the other 2 modules to: check the robots.txt and return a message, to scrape data from GradCafe.com, to load the raw data JSON file, to clean the file, and to save the final product as a JSON file.

I then run the LLM to clean the file further and save it as 'llm_extend_applicant_data.jsonl'. The LLM's output's file is in JSONL format and JSON format.



How to run the site:

-Install the required packages in the requirements.txt file
-In VS Code, run 'python run.py'
-Once receive the "clean_applicant_data.json", in VS Code, go to the folder where the 'app.py. is and run 'python app.py --file cleaned_applicant_data.json --stdout > llm_extend_applicant_data.jsonl'


SSH URL:
git@github.com:nampham929/jhu_software_concepts.git


4. Known Bugs: 
I ran into this error message 'HTTP Error 502: Bad Gateway' several times as the program ran. From my research, this is due to the GradCafe server stopped responding as too many requests were sent too fast. It took me several attempts to run my program before I could gather 32,000 records.

When I ran the LLM to 'UnicodeEncodeError:', this was caused because some comments inputted into GradCafe carried emojies as the LLM was printing JSONL format data to the output file. Therefore, I had to reconfigure the 'app.py' file in the LLM by adding sys.stdout.reconfigure(encoding="utf-8"), and that cleared the error.


5. Citations:


