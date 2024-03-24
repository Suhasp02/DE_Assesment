from sqlalchemy import *
import pandas as pd
import glob
import re
import yaml
import logging
import sys
import modules as pc
# Configure logging
logging.basicConfig(filename='data_processing.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def connect_to_db(user, password, host, port, dbname):
    """
         This UDF performs matching of all file name pattern and extract the date .

         Args:
             user: User_name for the Postgres DB
             password: Password for the DB
             host: Host name
             port: port number
             dbname: Database Name

         Returns:


         Log:
             Logging Info for all the actioned performed by the function
         Raises:
             ValueError: If invalid data is provided.
    """

    try:
        connection_string = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
        logging.info(f"Connected to database: {dbname} UserName : {user}")
        return create_engine(connection_string)
    except FileNotFoundError as e:
        logging.error(f"Error: YAML configuration file not found. {e}")
        raise
    except yaml.YAMLError as e:
        logging.error(f"Error: Failed to parse YAML configuration. {e}")
        raise


def split_filename(filename):
    """
         This UDF performs matching of all file name pattern and extract the date .

         Args:
             filename: Name of the file mentioned yaml

         Returns:
             name: filename , date: raw date , postgres_date: transformed date

         Log:
             Logging Info for all the actioned performed by the function
         """
    # Define regular expressions for the file patterns
    logging.info(f"Splitting the filename: {filename} to extract FundName and Date")
    pattern1 = r"(.*)\.(?P<date>\d{2}-\d{2}-\d{4})\ *.(.*)"  # filename.DD-MM-YYYY.extension
    pattern2 = r"(.*)\.(?P<date>\d{2}_\d{2}_\d{4})\.(.*)"  # filename.DD_MM_YYYY.extension
    pattern3 = r"(.*)\.(?P<date>\d{4}-\d{2}-\d{2})\.(.*)"  # filename.YYYY-MM-DD.extension
    pattern4 = r"(.*)\.(?P<date>\d{8})\.(.*)"  # filename.YYYYMMDD.extension
    pattern5 = r"(.*)\.(?P<date>\d{2}_\d{2}_\d{4})\.(?P<extension>\w+)"

    match1 = re.match(pattern1, filename)
    match2 = re.match(pattern2, filename)
    match3 = re.match(pattern3, filename)
    match4 = re.match(pattern4, filename)
    match5 = re.match(pattern5, filename)

    if match1:

        name = match1.group(1)
        date = match1.group('date')
        if re.search(r"(Gohen|Virtous)", filename):
            month, day, year = date.split('-')
        else:
            day, month, year = date.split('-')
        postgres_date = f"{year}-{month}-{day}"
        logging.info(f"Filename: {filename} {postgres_date} matches pattern1")
        return name, date, postgres_date

    elif match2:

        name = match2.group(1)
        date = match2.group('date')
        if re.search("Leeder", filename):
            month, day, year = date.split('_')
        else:
            day, month, year = date.split('_')
        postgres_date = f"{year}-{month}-{day}"
        logging.info(f"Filename: {filename} matches pattern2")
        return name, date, postgres_date

    elif match3:

        name = match3.group(1)
        date = match3.group('date')
        year, month, day = date.split('-')
        postgres_date = f"{year}-{month}-{day}"
        logging.info(f"Filename: {filename} matches pattern3")
        return name, date, postgres_date

    elif match4:

        date_str = match4.group('date')
        year = date_str[:4]
        month = date_str[4:6]
        day = date_str[6:]
        date = f"{year}-{month}-{day}"
        name = match4.group(1)
        postgres_date = f"{year}-{month}-{day}"
        logging.info(f"Filename: {filename} matches pattern4")
        return name, date, postgres_date

    elif match5:

        name = match5.group(1)
        date = match5.group('date')
        month, day, year = date.split('_')
        postgres_date = f"{year}-{month}-{day}"
        logging.info(f"Filename: {filename} matches pattern5")
        return name, date, postgres_date

    else:
        logging.info(f"Filename : {filename} doesn't match any  pattern")
        return None


def load_data(filename, f_name, engine, column_mapping, table_name):
    """
            This UDF performs data load to Postgres DB .

            Args:
                f_name: file name
                table_name: Table name in which data will get loaded
                column_mapping: Column mapping defined in yaml
                filename: Name of the file mentioned yaml
                engine: connection engine

            Raises:
             ValueError: If invalid data is provided.
            """
    try:
        df = pd.read_csv(filename)
        name, date, postgres_date = split_filename(f_name)
        df = df.reset_index(drop=True)

        num_columns = len(df.columns)
        num_mappings = len(column_mapping)

        if num_columns != num_mappings:
            logging.info(f"Number of columns in the DataFrame: {num_columns} does not match the number of column mappings {num_mappings}")
            raise

        new_columns = list(column_mapping)
        df.columns = new_columns
        rec_count = df.shape[0]
        new_data = {'fund_nm': name, 'file_nm': f_name, 'file_dt': postgres_date}
        df = df.assign(**new_data)

        logging.info(f"Loading {filename} into DB")

        df.to_sql(table_name, engine, if_exists='append', index=False)

        logging.info(f"Number of records loaded : {rec_count} into DB")
    except FileNotFoundError as e:
        logging.info(f"Error: CSV file not found - {filename}. {e}")
        raise
    except pd.errors.ParserError as e:
        logging.info(f"Error: Failed to parse CSV data - {filename}. {e}")
        raise


def main():
    """Main function to coordinate the process."""

    try:
        with open('config.yaml') as f:
            config = yaml.safe_load(f)

        logging.info(f"YAML file opened")
        db_config = config['db']
        host = db_config['host']
        port = db_config['port']
        dbname = db_config['dbname']
        user = db_config['user']
        password = db_config['password']
        table_name = config['table_nm']

        engine = connect_to_db(user, password, host, port, dbname)

        try:
            pc.create_table()
        except ValueError as e:
            sys.exit(f"Error: {e}")

        data_path = config['data_path']
        csv_files = glob.glob(f"{data_path}*.csv")

        column_mapping = config['column_mapping']

        for filename in csv_files:
            extracted_data = filename.split("/")
            f_name = extracted_data[-1]
            load_data(filename, f_name, engine, column_mapping, table_name)

        try:
            pc.recon()
            logging.info(f"Recon data view created/refreshed")
        except ValueError as e:
            sys.exit(f"Error: {e}")

        try:
            pc.equities_report()
            logging.info(f"Equities Report data view created/refreshed")
        except ValueError as e:
            sys.exit(f"Error: {e}")

        try:
            pc.export_report_data()
            logging.info(f"Report data extracted")
        except ValueError as e:
            sys.exit(f"Error: {e}")

        engine.dispose()
    except (FileNotFoundError, ValueError) as e:
        logging.info(f"An error occurred during processing: {e}")
        raise


if __name__ == "__main__":
    main()
