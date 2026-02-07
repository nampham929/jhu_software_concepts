import psycopg
from psycopg import OperationalError
import json

def create_connection(db_name, db_user, db_password, db_host, db_port):
    """Create a connection to PostgreSQL database."""
    connection = None
    try:
        connection = psycopg.connect(
            dbname=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        print("Connection to PostgreSQL DB successful")
    except OperationalError as e:
        print(f"The error '{e}' occurred")
        raise
    return connection

def create_applicants_table(connection):
    """Create the applicants table if it doesn't exist."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS applicants (
        p_id SERIAL PRIMARY KEY,
        program TEXT,
        comments TEXT,
        date_added DATE,
        url TEXT,
        status TEXT,
        term TEXT,
        us_or_international TEXT,
        gpa FLOAT,
        gre FLOAT,
        gre_v FLOAT,
        gre_aw FLOAT,
        degree TEXT,
        llm_generated_program TEXT,
        llm_generated_university TEXT
    );
    """
    try:
        connection.execute(create_table_query)
        connection.commit()
        print("Applicants table created successfully")
    except Exception as e:
        print(f"Error creating table: {e}")
        connection.rollback()
        raise

def parse_date(date_string):
    """Parse date string in format 'Month DD, YYYY' to 'YYYY-MM-DD'."""
    if not date_string:
        return None
    try:
        from datetime import datetime
        dt = datetime.strptime(date_string, "%B %d, %Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        print(f"Warning: Could not parse date '{date_string}'")
        return None

def parse_float(value):
    """Parse float value, return None if empty or invalid."""
    if value is None or value == "":
        return None
    try:
        return float(value)
    except ValueError:
        return None

def detect_file_encoding(file_path):
    """Detect file encoding by BOM; fall back to utf-8 if unknown."""
    with open(file_path, "rb") as f:
        first_bytes = f.read(4)
    if first_bytes.startswith(b"\xff\xfe") or first_bytes.startswith(b"\xfe\xff"):
        return "utf-16"
    if first_bytes.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"
    return "utf-8"

def load_data_from_jsonl(connection, jsonl_file):
    """Load data from JSONL file into the applicants table."""
    try:
        inserted_count = 0
        error_count = 0
        first_error_line = None
        first_error_message = None

        encoding = detect_file_encoding(jsonl_file)
        print(f"Detected file encoding: {encoding}")

        with open(jsonl_file, 'r', encoding=encoding) as file_handle:
            for line_num, line in enumerate(file_handle, 1):
                if not line.strip():  # Skip empty lines
                    continue
                    
                try:
                    data = json.loads(line.strip())
                    
                    # Parse and map the data to table columns
                    program = data.get('program')
                    comments = data.get('comments')
                    date_added = parse_date(data.get('date_added'))
                    url = data.get('url')
                    status = data.get('status')
                    term = data.get('term')
                    us_or_international = data.get('US/International')
                    gpa = parse_float(data.get('GPA'))
                    gre = parse_float(data.get('GRE_SCORE'))
                    gre_v = parse_float(data.get('GRE_V'))
                    gre_aw = parse_float(data.get('GRE_AW'))
                    degree = data.get('Degree')
                    llm_generated_program = data.get('llm-generated-program')
                    llm_generated_university = data.get('llm-generated-university')
                    
                    # Insert into database
                    insert_query = """
                    INSERT INTO applicants (
                        program, comments, date_added, url, status, term, 
                        us_or_international, gpa, gre, gre_v, gre_aw, 
                        degree, llm_generated_program, llm_generated_university
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    
                    connection.execute(insert_query, (
                        program, comments, date_added, url, status, term,
                        us_or_international, gpa, gre, gre_v, gre_aw,
                        degree, llm_generated_program, llm_generated_university
                    ))
                    inserted_count += 1
                    
                    # Commit every 100 records for efficiency
                    if inserted_count % 100 == 0:
                        connection.commit()
                        print(f"Inserted {inserted_count} records...")
                        
                except json.JSONDecodeError as e:
                    error_count += 1
                    if first_error_line is None:
                        first_error_line = line_num
                        first_error_message = f"JSON decode error: {e}"
                    if error_count <= 5:  # Only print first 5 errors
                        print(f"Warning: Could not parse JSON at line {line_num}: {e}")
                        print(f"Line content: {line[:100]}...")  # Print first 100 chars
                    continue
                except Exception as e:
                    error_count += 1
                    if first_error_line is None:
                        first_error_line = line_num
                        first_error_message = f"Insert error: {e}"
                    if error_count <= 5:  # Only print first 5 errors
                        print(f"Error inserting record at line {line_num}: {e}")
                    connection.rollback()
                    continue
        
        # Final commit
        connection.commit()
        print(f"Data loading completed. Total records inserted: {inserted_count}")
        if error_count > 0:
            print(f"Total errors encountered: {error_count}")
            if first_error_line is not None:
                print(f"First error at line {first_error_line} ({first_error_message})")
        
    except FileNotFoundError:
        print(f"Error: File '{jsonl_file}' not found")
        raise
    except Exception as e:
        print(f"Error during data loading: {e}")
        connection.rollback()
        raise

def query_first_row(connection):
    """Query and display the first row from the applicants table."""
    try:
        cursor = connection.execute("SELECT * FROM applicants ORDER BY p_id LIMIT 1;")
        row = cursor.fetchone()
        if row:
            print("\n--- First Row in Database ---")
            date_added = row[3]
            date_added_display = date_added.isoformat() if hasattr(date_added, "isoformat") else date_added
            print(f"Date added: {date_added_display}")
            print(f"Status: {row[5]}")  # status is the 6th column (index 5)
            print(f"Full row data: {row}")
        else:
            print("No data found in the table.")
    except Exception as e:
        print(f"Error querying data: {e}")

def main():
    """Main function to orchestrate the data loading process."""
    # Database configuration - replace with your actual credentials
    db_name = "postgres"
    db_user = "postgres"
    db_password = "dataBase!605"
    db_host = "localhost"
    db_port = "5432"
    
    # Path to the JSONL file
    jsonl_file = "llm_extend_applicant_data.jsonl"
    
    try:
        # Create connection
        conn = create_connection(db_name, db_user, db_password, db_host, db_port)
        
        # Create table
        create_applicants_table(conn)
        
        # Load data
        load_data_from_jsonl(conn, jsonl_file)
        
        # Query and display first row
        query_first_row(conn)
        
        # Close connection
        conn.close()
        print("Connection closed.")
        
    except Exception as e:
        print(f"Failed to complete data loading: {e}")

if __name__ == "__main__":
    main()
