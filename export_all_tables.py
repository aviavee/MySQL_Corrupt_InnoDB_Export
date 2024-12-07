import mysql.connector  
from mysql.connector import Error  
import csv  
import os  
import sys  
import argparse  
import signal  

def export_all_tables_in_batches(db_name, output_dir, username, password, batch_size=10000, single_query_mode=False):  
    """  
    Export all tables in a MySQL database to CSV files in batches or one query per ID.  
    If the program is restarted, it resumes from where it left off.  
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
                print("Connected to MySQL server.")  
                return connection  
        except Error as e:  
            print(f"Error connecting to MySQL: {e}")  
            sys.exit(1)  

    def get_last_exported_id(output_file):  
        """  
        Efficiently read the last line of the output file to determine the last exported ID.  
        """  
        if not os.path.exists(output_file):  
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
                return None  # File is empty or last line is blank  

            last_row = last_line.split(",")  # Assuming CSV format  
            try:  
                return int(last_row[0])  # Assuming the first column is the primary key  
            except ValueError:  
                print("Error: Malformed last line in the output file.")  
                sys.exit(1)  # Quit the program if the last line is malformed  

    def check_missing_rows(cursor, table_name, primary_key_column, last_id):  
        """  
        Check if there are rows in the database that have not been exported yet.  
        """  
        try:  
            query = f"""  
                SELECT COUNT(*) FROM {table_name}  
                WHERE {primary_key_column} > {last_id}  
            """  
            cursor.execute(query)  
            result = cursor.fetchone()  
            return result[0] > 0  # Return True if there are rows to export  
        except Error as e:  
            print(f"Error checking for missing rows in table {table_name}: {e}")  
            sys.exit(1)  

    def export_single_query(cursor, table_name, primary_key_column, current_id):  
        """  
        Export a single row for the given ID.  
        """  
        try:  
            query = f"""  
                SELECT * FROM {table_name}  
                WHERE {primary_key_column} = {current_id}  
            """  
            cursor.execute(query)  
            row = cursor.fetchone()  
            return row  
        except Error as e:  
            print(f"Error exporting row with ID {current_id} from table {table_name}: {e}")  
            sys.exit(1)  

    def export_batch(cursor, table_name, primary_key_column, last_id, batch_size):  
        """  
        Export a batch of data starting from the last exported ID.  
        """  
        try:  
            query = f"""  
                SELECT * FROM {table_name}  
                WHERE {primary_key_column} > {last_id}  
                ORDER BY {primary_key_column} ASC  
                LIMIT {batch_size}  
            """  
            cursor.execute(query)  
            rows = cursor.fetchall()  
            return rows  
        except Error as e:  
            print(f"Error exporting batch from table {table_name} starting from ID {last_id}: {e}")  
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
                return result[4]  # The 5th column in the result is the column name  
            else:  
                print(f"Error: Table {table_name} does not have a primary key.")  
                sys.exit(1)  
        except Error as e:  
            print(f"Error retrieving primary key for table {table_name}: {e}")  
            sys.exit(1)  

    def get_all_tables(cursor):  
        """  
        Retrieve all table names in the database.  
        """  
        try:  
            cursor.execute("SHOW TABLES")  
            tables = cursor.fetchall()  
            return [table[0] for table in tables]  
        except Error as e:  
            print(f"Error retrieving tables: {e}")  
            sys.exit(1)  

    def graceful_exit(signal_received, frame):  
        """  
        Handle graceful exit on Ctrl-C.  
        """  
        print("\nGracefully exiting...")  
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
            print(f"Starting export for table: {table_name}")  

            # Determine the output file for the table  
            output_file = os.path.join(output_dir, f"{table_name}.csv")  

            # Get the primary key column for the table  
            primary_key_column = get_primary_key_column(cursor, table_name)  

            # Determine the starting point  
            last_exported_id = get_last_exported_id(output_file)  
            if last_exported_id is not None:  
                start_id = last_exported_id  
                print(f"Resuming export for table {table_name} from ID: {start_id}")  
            else:  
                start_id = 0  
                print(f"Starting export for table {table_name} from ID: {start_id}")  

            # Check if there are rows to export  
            if not check_missing_rows(cursor, table_name, primary_key_column, start_id):  
                print(f"All rows already exported for table: {table_name}")  
                continue  

            # Open the CSV file for appending  
            with open(output_file, mode='a', newline='', encoding='utf-8') as file:  
                writer = csv.writer(file)  

                # Export data  
                if single_query_mode:  
                    # Single query mode: Export one row at a time  
                    current_id = start_id + 1  
                    while True:  
                        print(f"Exporting row with ID: {current_id} from table {table_name}...")  
                        row = export_single_query(cursor, table_name, primary_key_column, current_id)  

                        if not row:  
                            print(f"No more rows to export for table: {table_name}")  
                            break  

                        writer.writerow(row)  
                        current_id += 1  
                else:  
                    # Batch mode: Export data in batches  
                    while True:  
                        print(f"Exporting batch for table {table_name} starting from ID: {start_id + 1}...")  
                        rows = export_batch(cursor, table_name, primary_key_column, start_id, batch_size)  

                        if not rows:  
                            print(f"Export complete for table: {table_name}")  
                            break  

                        writer.writerows(rows)  
                        start_id = rows[-1][0]  # Update the last exported ID (assuming the first column is the primary key)  

        print("All tables exported successfully.")  

    except Error as e:  
        print(f"Error: {e}")  
        sys.exit(1)  # Quit the program if a critical error occurs  

    finally:  
        if connection.is_connected():  
            cursor.close()  
            connection.close()  
            print("MySQL connection closed.")  

if __name__ == "__main__":  
    # Set up argument parsing  
    parser = argparse.ArgumentParser(description="Export all tables from a MySQL database to CSV files in batches or one query per ID.")  
    parser.add_argument("--db_name", required=True, help="Name of the MySQL database to export.")  
    parser.add_argument("--output_dir", required=True, help="Directory to save the exported CSV files.")  
    parser.add_argument("--username", required=True, help="MySQL username.")  
    parser.add_argument("--password", required=True, help="MySQL password.")  
    parser.add_argument("--batch_size", type=int, default=10000, help="Number of rows to export in each batch (default: 10000).")  
    parser.add_argument("--single_query_mode", action="store_true", help="Enable single query mode to export one row at a time.")  

    # Parse the arguments  
    args = parser.parse_args()  

    # Run the export function with the provided arguments  
    export_all_tables_in_batches(  
        db_name=args.db_name,  
        output_dir=args.output_dir,  
        username=args.username,  
        password=args.password,  
        batch_size=args.batch_size,  
        single_query_mode=args.single_query_mode  
    )  
