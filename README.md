# MySQL Database Exporter  

This script exports all tables from a MySQL database to CSV files in batches. It is designed to handle large databases efficiently and can resume from where it left off in case of interruptions. Based on my experience, this approach has a **higher probability of success** when dealing with a corrupted database, as it processes data in smaller chunks and avoids loading large amounts of data into memory at once.  

## Features  
- Exports all tables in a MySQL database to individual CSV files.  
- Resumes from the last exported row if the program is interrupted or restarted.  
- Handles large tables efficiently by exporting data in batches.  
- Automatically detects the primary key of each table for incremental exports.  
- Allows customization of batch size via command-line arguments.  

## Requirements  
- Python 3.x  
- MySQL Connector for Python (`mysql-connector-python`)  

## Installation  
1. Clone this repository:  
   ```bash  
   git clone https://github.com/your-username/mysql-database-exporter.git  
   cd mysql-database-exporter  
   ```  

2. Install the required Python package:  
   ```bash  
   pip install mysql-connector-python  
   ```  

## Usage  
Run the script with the following command-line arguments:  

```bash  
python export_all_tables.py --db_name <DATABASE_NAME> --output_dir <OUTPUT_DIRECTORY> --username <MYSQL_USERNAME> --password <MYSQL_PASSWORD> [--batch_size <BATCH_SIZE>]  
```  

## Arguments  
- `--db_name`: Name of the MySQL database to export (required).  
- `--output_dir`: Directory to save the exported CSV files (required).  
- `--username`: MySQL username (required).  
- `--password`: MySQL password (required).  
- `--batch_size`: Number of rows to export in each batch (optional, default: 10000).  

## Example  
```bash  
python export_all_tables.py --db_name aircraft --output_dir exports --username root --password mypassword --batch_size 5000  
```  

This will export all tables from the `aircraft` database to the `exports` directory, processing 5000 rows at a time.  

## Why This Approach Works Well for Corrupted Databases  
Based on my experience, this approach has a **higher probability of success** when dealing with corrupted databases because:  
1. It processes data in smaller chunks (batches), reducing the risk of memory overload or timeouts.  
2. It resumes from the last successfully exported row, minimizing data loss in case of interruptions.  
3. It avoids loading the entire table into memory, which is especially useful for large or partially corrupted tables.  

## Notes  
- Ensure that the MySQL user has sufficient privileges to read from the database.  
- The script assumes that each table has a primary key. If a table does not have a primary key, the script will terminate with an error.  

## License  
This project is licensed under the MIT License. See the LICENSE file for details.  
