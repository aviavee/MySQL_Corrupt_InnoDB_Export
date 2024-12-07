# MySQL Database Export Tool  

## Description  
A robust Python utility designed to export data from corrupted MySQL databases. This tool provides flexible options for data extraction, including batch processing and single-query modes, with built-in resume capability and error handling.  

## Features  
- Batch export or single-query export modes  
- Resume capability from last exported ID  
- Configurable batch sizes  
- Selective table export/skip functionality  
- Detailed logging  
- Graceful exit handling  
- Support for corrupted database recovery  

## Installation  

``` bash  
pip install mysql-connector-python  
```  

## Usage  

``` bash  
python mysql_export.py --db_name DATABASE --output_dir OUTPUT_DIR --username USER --password PASS [options]  
```  

## Arguments  
- `--db_name`: Name of MySQL database to export (required)  
- `--output_dir`: Directory for CSV output files (required)  
- `--username`: MySQL username (required)  
- `--password`: MySQL password (required)  
- `--batch_size`: Number of rows per batch (default: 10000)  
- `--single_query_mode`: Enable row-by-row export  
- `--log_level`: Set logging level (DEBUG/INFO/WARNING/ERROR/CRITICAL)  
- `--last_id`: Manually specify last ID for each table  
- `--skip_tables`: List of tables to skip during export  

## Core Functions  

### export_all_tables_in_batches  
Main function orchestrating the export process. Handles database connection, table iteration, and export coordination.  

### connect_to_db  
Establishes MySQL database connection with error handling.  

### get_last_exported_id  
Efficiently determines the last exported ID from output files for resume functionality.  

### export_single_query  
Exports individual rows in single-query mode, useful for corrupted databases.  

### export_batch  
Handles batch exports of data with configurable batch sizes.  

### get_primary_key_column  
Retrieves primary key information for each table.  

### get_all_tables  
Fetches complete list of database tables.  

### graceful_exit  
Handles clean program termination on interruption.  

## Error Handling  
- Comprehensive error logging  
- Graceful exit on critical errors  
- Resume capability on interruption  
- Validation of database connections  
- Primary key verification  

## Output  
- Creates CSV files for each table  
- Maintains data integrity  
- Supports UTF-8 encoding  
- Includes progress logging  

## Example  

``` bash  
python mysql_export.py \  
  --db_name my_database \  
  --output_dir ./exports \  
  --username root \  
  --password mypass \  
  --batch_size 5000 \  
  --log_level DEBUG \  
  --skip_tables table1 table2  
```  

## License  
MIT License  

## Author  
[Your Name]  
