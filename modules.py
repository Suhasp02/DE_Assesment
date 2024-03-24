import pandas as pd
import psycopg2
from psycopg2 import sql
import yaml
import logging
import sys

logging.basicConfig(filename='data_processing.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

with open('config.yaml') as f:
    config = yaml.safe_load(f)

db_config = config['db']
host = db_config['host']
port = db_config['port']
dbname = db_config['dbname']
user = db_config['user']
password = db_config['password']

ddl = config['ddl']
table_name = config['table_nm']

view_ddl = config['recon_view']
recon_view_name = config['recon_view_name']
recon_cols = config['recon_report_cols']

report_view = config['report_view']
report_name = config['report_name']
report_cols = config['best_fund_report_cols']

rep_path = config['rep_path']

conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)


def create_table():
    """
        This UDF performs table creation on the provided data.

    """
    try:
        cur = conn.cursor()

        cur.execute("SELECT * FROM information_schema.tables WHERE table_name = %s", (table_name,))
        if bool(cur.rowcount):
            logging.info("Table exists in the database.")
        else:
            logging.error("Table not found in the database.")
            cur.execute(ddl)
            conn.commit()
            logging.info("DDL execution successful!")

    except (FileNotFoundError, ValueError) as e:
        logging.info(f"An error occurred during processing: {e}")
        raise ValueError("An error occurred during processing")


def recon():
    """
         This UDF performs reconcilliation of table data.

    """
    try:
        cur = conn.cursor()

        cur.execute("SELECT * FROM information_schema.views WHERE table_name = %s", (recon_view_name,))
        if bool(cur.rowcount):
            logging.info("View exists in the database.")
        else:
            logging.error("View not found in the database.")
            cur.execute(view_ddl)
            conn.commit()
            logging.info("View creation successful!")

    except (FileNotFoundError, ValueError) as e:
        logging.info(f"An error occurred during processing: {e}")
        raise ValueError("An error occurred during processing")


def equities_report():
    """
         This UDF performs equity report creation.
    """
    try:
        cur = conn.cursor()

        cur.execute("SELECT * FROM information_schema.views WHERE table_name = %s", (report_name,))
        if bool(cur.rowcount):
            logging.info("View exists in the database.")
        else:
            logging.error("View not found in the database.")
            cur.execute(report_view)
            conn.commit()
            logging.info("View creation successful!")

    except (FileNotFoundError, ValueError) as e:
        logging.info(f"An error occurred during processing: {e}")
        raise ValueError("An error occurred during processing")

def export_report_data():

    try:
        cur = conn.cursor()
        cur.execute(sql.SQL("SELECT * FROM {table}").format(table=sql.Identifier(recon_view_name)))
        data = cur.fetchall()  # Fetch all results
        df = pd.DataFrame(data, columns=[col.name for col in cur.description])
        df.to_csv(rep_path+'recon_report.csv', index=False)
    except Exception as e:
        logging.info(f"Error executing query: {e}")
        exit()

    try:
        cur = conn.cursor()
        cur.execute(sql.SQL("SELECT * FROM {table}").format(table=sql.Identifier(report_name)))
        data1 = cur.fetchall()  # Fetch all results
        df1 = pd.DataFrame(data1, columns=[col.name for col in cur.description])
        df1.to_csv(rep_path+'best_fund_report.csv', index=False)
    except Exception as e:
        logging.info(f"Error executing query: {e}")
        exit()
