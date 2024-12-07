import mysql.connector  
from mysql.connector import Error  
import csv  
import os  
import sys  
import argparse  
import signal  
import logging  

def export_all_tables_in_batches(db_name, output_dir, username, password, batch_size=10000, single_query_mode=False, use_last_id=False, skip_tables=None):  
    """  
    Export all tables in a MySQL database to CSV files in batches or one query per ID.  
    If the program is restarted, it resumes from where it left off.  

    Args:  
        ...existing args...  
        skip_tables (list): List of table names to skip during export  
    """ 
    def connect_to_db():  
        """Establish a connection to the MySQL database."""  
        try:  
            connection = mysql.connector.connect(  
                host='localhost',  # Assuming the server is localhost  
                user=username,  
                password=password,  
                database=db_name  
            )  
            if connection.is_connected():  
                logging.info("Connected to MySQL server.")  
                return connection  
        except Error as e:  
            logging.error(f"Error connecting to MySQL: {e}")  
            sys.exit(1)  

    def get_last_exported_id(output_file):  
        """  
        Efficiently read the last line of the output file to determine the last exported ID.  
        """  
        if not os.path.exists(output_file):  
            logging.info(f"Output file {output_file} does not exist. Starting from the beginning.")  
            return None  # File doesn't exist, start from the beginning  

        with open(output_file, 'rb') as file:  # Open in binary mode for efficient seeking  
            try:  
                file.seek(-2, os.SEEK_END)  # Move the pointer to the second-to-last byte  
                while file.read(1) != b'\n':  # Read backwards until a newline is found  
                    file.seek(-2, os.SEEK_CUR)  
            except OSError:  
                file.seek(0)  # If the file is too small, go to the beginning  

            last_line = file.readline().decode('utf-8')  # Read the last line and decode it  
            if not last_line.strip():  
                logging.info(f"Output file {output_file} is empty. Starting from the beginning.")  
                return None  # File is empty or last line is blank  

            last_row = last_line.split(",")  # Assuming CSV format  
            try:  
                last_id = int(last_row[0])  # Assuming the first column is the primary key  
                logging.debug(f"Last exported ID from file {output_file}: {last_id}")  
                return last_id  
            except ValueError:  
                logging.error(f"Malformed last line in the output file {output_file}.")  
                sys.exit(1)  # Quit the program if the last line is malformed  

    def export_single_query(cursor, table_name, primary_key_column, current_id):  
        """  
        Export a single row for the given ID.  
        If the ID does not exist, return None to indicate it should be skipped.  
        """  
        try:  
            query = f"""  
                SELECT * FROM {table_name}  
                WHERE {primary_key_column} = {current_id}  
            """  
            cursor.execute(query)  
            row = cursor.fetchone()  
            if row:  
                logging.debug(f"Row with ID {current_id} found in table {table_name}.")  
            else:  
                logging.debug(f"Row with ID {current_id} does not exist in table {table_name}.")  
            return row  
        except Error as e:  
            logging.error(f"Error exporting row with ID {current_id} from table {table_name}: {e}")  
            sys.exit(1)  

    def export_batch(cursor, table_name, primary_key_column, last_id, batch_size, max_id=None):  
        """  
        Export a batch of data starting from the last exported ID.  
        If max_id is provided, only export rows up to max_id.  
        """  
        try:  
            query = f"""  
                SELECT * FROM {table_name}  
                WHERE {primary_key_column} > {last_id}  
            """  
            if max_id is not None:  
                query += f" AND {primary_key_column} <= {max_id}"  
            query += f" ORDER BY {primary_key_column} ASC LIMIT {batch_size}"  

            cursor.execute(query)  
            rows = cursor.fetchall()  
            logging.debug(f"Exported batch of {len(rows)} rows from table {table_name} starting from ID {last_id + 1}.")  
            return rows  
        except Error as e:  
            logging.error(f"Error exporting batch from table {table_name} starting from ID {last_id}: {e}")  
            sys.exit(1)  

    def get_primary_key_column(cursor, table_name):  
        """  
        Get the primary key column of a table.  
        """  
        try:  
            query = f"""  
                SHOW KEYS FROM {table_name} WHERE Key_name = 'PRIMARY'  
            """  
            cursor.execute(query)  
            result = cursor.fetchone()  
            if result:  
                primary_key = result[4]  # The 5th column in the result is the column name  
                logging.debug(f"Primary key for table {table_name}: {primary_key}")  
                return primary_key  
            else:  
                logging.error(f"Table {table_name} does not have a primary key.")  
                sys.exit(1)  
        except Error as e:  
            logging.error(f"Error retrieving primary key for table {table_name}: {e}")  
            sys.exit(1)  

    def get_all_tables(cursor):  
        """  
        Retrieve all table names in the database.  
        """  
        try:  
            cursor.execute("SHOW TABLES")  
            tables = cursor.fetchall()  
            table_names = [table[0] for table in tables]  
            logging.debug(f"Tables in database: {table_names}")  
            return table_names  
        except Error as e:  
            logging.error(f"Error retrieving tables: {e}")  
            sys.exit(1)  

    def graceful_exit(signal_received, frame):  
        """  
        Handle graceful exit on Ctrl-C.  
        """  
        logging.info("Gracefully exiting...")  
        sys.exit(0)  

    # Register the signal handler for Ctrl-C  
    signal.signal(signal.SIGINT, graceful_exit)  

    try:  
        # Step 1: Connect to the database  
        connection = connect_to_db()  
        cursor = connection.cursor()  

        # Step 2: Get all table names  
        tables = get_all_tables(cursor)  

        # Step 3: Export each table  
        for table_name in tables:  
            # Skip tables if they're in the skip_tables list  
            if skip_tables and table_name in skip_tables:  
                logging.info(f"Skipping table: {table_name} (specified in skip_tables)")  
                continue 

            logging.info(f"Starting export for table: {table_name}")  

            # Determine the output file for the table  
            output_file = os.path.join(output_dir, f"{table_name}.csv")  

            # Get the primary key column for the table  
            primary_key_column = get_primary_key_column(cursor, table_name)  

            # Determine the starting point  
            start_id = get_last_exported_id(output_file)  
            if start_id is not None:  
                logging.info(f"Resuming export for table {table_name} from ID: {start_id}")  
            else:  
                start_id = 0  
                logging.info(f"Starting export for table {table_name} from ID: {start_id}")  

            # If --last_id is set, prompt the user for the last ID in the database  
            max_id = None  
            if use_last_id:  
                try:  
                    max_id = int(input(f"Enter the last ID that exists in the database for table {table_name}: "))  
                    logging.info(f"Using manually specified last ID ({max_id}) for table {table_name}.")  
                except ValueError:  
                    logging.error("Invalid input. Please enter a valid integer for the last ID.")  
                    sys.exit(1)  

            # Open the CSV file for appending  
            with open(output_file, mode='a', newline='', encoding='utf-8') as file:  
                writer = csv.writer(file)  

                if single_query_mode:  
                    # Single query mode: Export one row at a time  
                    current_id = start_id + 1  
                    while current_id <= max_id:  # Changed condition to continue until max_id  
                        logging.info(f"Exporting row with ID: {current_id} from table {table_name}...")  
                        row = export_single_query(cursor, table_name, primary_key_column, current_id)  

                        if row:  
                            writer.writerow(row)  
                        else:  
                            logging.info(f"ID {current_id} does not exist. Skipping...")  

                        current_id += 1  

                    logging.info(f"Reached the manually specified last ID ({max_id}) for table {table_name}.")  
                else:  
                    # Batch mode: Export data in batches  
                    while True:  
                        logging.info(f"Exporting batch for table {table_name} starting from ID: {start_id + 1}...")  
                        rows = export_batch(cursor, table_name, primary_key_column, start_id, batch_size, max_id=max_id)  

                        if not rows:  
                            logging.info(f"Export complete for table: {table_name}")  
                            break  

                        writer.writerows(rows)  
                        start_id = rows[-1][0]  # Update the last exported ID (assuming the first column is the primary key)  

        logging.info("All tables exported successfully.")  

    except Error as e:  
        logging.error(f"Error: {e}")  
        sys.exit(1)  # Quit the program if a critical error occurs  

    finally:  
        if connection.is_connected():  
            cursor.close()  
            connection.close()  
            logging.info("MySQL connection closed.")  

if __name__ == "__main__":  
    # Set up argument parsing  
    parser = argparse.ArgumentParser(description="Export all tables from a MySQL database to CSV files in batches or one query per ID.")  
    parser.add_argument("--db_name", required=True, help="Name of the MySQL database to export.")  
    parser.add_argument("--output_dir", required=True, help="Directory to save the exported CSV files.")  
    parser.add_argument("--username", required=True, help="MySQL username.")  
    parser.add_argument("--password", required=True, help="MySQL password.")  
    parser.add_argument("--batch_size", type=int, default=10000, help="Number of rows to export in each batch (default: 10000).")  
    parser.add_argument("--single_query_mode", action="store_true", help="Enable single query mode to export one row at a time.")  
    parser.add_argument("--log_level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], help="Set the logging level (default: INFO).")  
    parser.add_argument("--last_id", action="store_true", help="Manually specify the last ID that exists in the database for each table.")  
    # Add new argument for skip_tables  
    parser.add_argument("--skip_tables", nargs="+", help="List of table names to skip during export")  

    # Parse the arguments  
    args = parser.parse_args()  

    # Configure logging  
    logging.basicConfig(  
        level=getattr(logging, args.log_level.upper(), "INFO"),  
        format="%(asctime)s - %(levelname)s - %(message)s"  
    )  

    # Run the export function with the provided arguments  
    export_all_tables_in_batches(  
        db_name=args.db_name,  
        output_dir=args.output_dir,  
        username=args.username,  
        password=args.password,  
        batch_size=args.batch_size,  
        single_query_mode=args.single_query_mode,  
        use_last_id=args.last_id,  
        skip_tables=args.skip_tables  # Add skip_tables argument  
    )  
