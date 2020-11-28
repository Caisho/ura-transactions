import os
import logging
import psycopg2 as pg
from dotenv import load_dotenv

LOGGER = logging.getLogger(__name__)

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


def create_projects_table():
    query = (
        'CREATE TABLE public.private_residential_property_projects ('
        '   project varchar NOT NULL,'
        '   street varchar NOT NULL,'
        '   x varchar NULL,'
        '   y varchar NULL,'
        '   latitude varchar NULL,'
        '   longitude varchar NULL,'
        '   CONSTRAINT private_residential_property_projects_pk PRIMARY KEY (project, street)'
        ');'
    )
    conn = None
    try:
        conn = pg.connect(**params)
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()
        cur.close()
    except (pg.Error) as e:
        print(e)
    finally:
        if conn is not None:
            conn.close()


def create_transactions_table():
    query1 = (
        'CREATE TABLE public.private_residential_property_transactions ('
        '   project varchar NOT NULL,'
        '   street varchar NOT NULL,'
        '   area varchar NOT NULL,'
        '   floor_range varchar NOT NULL,'
        '   no_of_units varchar NULL,'
        '   contract_date date NOT NULL,'
        '   type_of_sale varchar NOT NULL,'
        '   price varchar NOT NULL,'
        '   property_type varchar NULL,'
        '   district varchar NULL,'
        '   type_of_area varchar NULL,'
        '   tenure varchar NULL,'
        '   psf varchar NULL,'
        '   CONSTRAINT private_residential_property_transactions_pk PRIMARY KEY ('
        '       project, street, area, floor_range,'
        '       contract_date, type_of_sale, price'
        '   )'
        ');'
    )
    query2 = (
        'ALTER TABLE public.private_residential_property_transactions '
        'ADD CONSTRAINT private_residential_property_transactions_fk FOREIGN KEY (project, street) '
        'REFERENCES private_residential_property_projects(project, street);'
    )
    conn = None
    try:
        conn = pg.connect(**params)
        cur = conn.cursor()
        cur.execute(query1)
        cur.execute(query2)
        conn.commit()
        cur.close()
    except (pg.Error) as e:
        print(e)
    finally:
        if conn is not None:
            conn.close()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    create_projects_table()
    create_transactions_table()
