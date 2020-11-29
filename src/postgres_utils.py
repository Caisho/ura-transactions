import os
from dotenv import load_dotenv
import psycopg2 as pg
import pandas as pd

load_dotenv()
POSTGRES_DB = os.getenv('POSTGRES_DB')
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')

params = {
    'host': POSTGRES_HOST,
    'port': POSTGRES_PORT,
    'user': POSTGRES_USER,
    'password': POSTGRES_PASSWORD,
    'dbname': POSTGRES_DB
}

def get_transactions_data():
    query = (
        'SELECT * '
        'FROM private_residential_property_transactions;'
    )
    conn = None
    df = None
    try: 
        conn = pg.connect(**params)
        df = pd.read_sql(query, con=conn)
    except (pg.Error) as e:
        print(e)
    finally:
        if conn:
            conn.close()
        return df


def get_contract_date_years():
    query = ('SELECT DISTINCT EXTRACT(YEAR FROM contract_date) as year FROM private_residential_property_transactions order by year asc')
    conn = None
    records = None
    try:
        conn = pg.connect(**params)
        cur = conn.cursor()
        cur.execute(query)
        records = cur.fetchall()
    except (pg.Error) as e:
        print(e)
    finally:
        if conn:
            cur.close()
            conn.close()
        return [int(element) for tupl in records for element in tupl]


def get_property_type_labels():
    query = ('SELECT DISTINCT property_type FROM private_residential_property_transactions order by property_type asc')
    conn = None
    records = None
    try:
        conn = pg.connect(**params)
        cur = conn.cursor()
        cur.execute(query)
        records = cur.fetchall()
    except (pg.Error) as e:
        print(e)
    finally:
        if conn:
            cur.close()
            conn.close()
        return [element for tupl in records for element in tupl]


def get_tenure_type_labels():
    query = ('SELECT DISTINCT tenure_type FROM private_residential_property_transactions order by tenure_type asc')
    conn = None
    records = None
    try:
        conn = pg.connect(**params)
        cur = conn.cursor()
        cur.execute(query)
        records = cur.fetchall()
    except (pg.Error) as e:
        print(e)
    finally:
        if conn:
            cur.close()
            conn.close()
        return [element for tupl in records for element in tupl]
