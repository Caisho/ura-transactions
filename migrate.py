import os
import logging
import psycopg2 as pg
from dotenv import load_dotenv

LOGGER = logging.getLogger(__name__)

load_dotenv()
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'postgres')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'postgres')
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')

params = {
    'host': POSTGRES_HOST,
    'port': POSTGRES_PORT,
    'user': POSTGRES_USER,
    'password': POSTGRES_PASSWORD,
    'dbname': POSTGRES_DB
}


def create_mrt_table():
    query = (
        'CREATE TABLE public.mrt ('
        '    id varchar NOT NULL,'
        '    name varchar NOT NULL,'
        '    type varchar NULL,'
        '    line varchar NULL,'
        '    longitude numeric NOT NULL,'
        '    latitude numeric NOT NULL,'
        '    CONSTRAINT mrt_pk PRIMARY KEY (name, id)'
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
        LOGGER.exception(e)
    finally:
        if conn is not None:
            conn.close()


def create_postal_districts_table():
    query = (
        'CREATE TABLE public.postal_districts ('
        '    name varchar NOT NULL,'
        '    latitude numeric NOT NULL,'
        '    longitude numeric NOT NULL,'
        '    postal_sector varchar NULL,'
        '    location varchar NULL,'
        '    CONSTRAINT postal_districts_pk PRIMARY KEY (name)'
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
        LOGGER.exception(e)
    finally:
        if conn is not None:
            conn.close()


def create_projects_table():
    query1 = (
        'CREATE TABLE public.private_residential_property_projects ('
        '    project varchar NOT NULL,'
        '    street varchar NOT NULL,'
        '    x numeric NULL,'
        '    y numeric NULL,'
        '    latitude numeric NULL,'
        '    longitude numeric NULL,'
        '    mrt_id varchar NULL,'
        '    mrt_name varchar NULL,'
        '    mrt_dist numeric NULL,'
        '    CONSTRAINT private_residential_property_projects_pk PRIMARY KEY (project, street)'
        ');'
    )
    query2 = (
        'ALTER TABLE public.private_residential_property_projects '
        'ADD CONSTRAINT private_residential_property_projects_fk FOREIGN KEY (mrt_name, mrt_id) '
        'REFERENCES mrt(name, id);'
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
        LOGGER.exception(e)
    finally:
        if conn is not None:
            conn.close()


def create_transactions_table():
    query1 = (
        'CREATE TABLE public.private_residential_property_transactions ('
        '   project varchar NOT NULL,'
        '   street varchar NOT NULL,'
        '   area numeric NOT NULL,'
        '   floor_range varchar NOT NULL,'
        '   no_of_units numeric NULL,'
        '   contract_date date NOT NULL,'
        '   type_of_sale varchar NOT NULL,'
        '   price numeric NOT NULL,'
        '   property_type varchar NULL,'
        '   district varchar NULL,'
        '   type_of_area varchar NULL,'
        '   tenure varchar NULL,'
        '   psf numeric NULL,'
        '   tenure_type varchar NULL,'
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
    query3 = (
        'CREATE INDEX private_residential_property_transactions_project_idx ON public.private_residential_property_transactions USING btree (project, street);'
    )
    conn = None
    try:
        conn = pg.connect(**params)
        cur = conn.cursor()
        cur.execute(query1)
        cur.execute(query2)
        cur.execute(query3)
        conn.commit()
        cur.close()
    except (pg.Error) as e:
        LOGGER.exception(e)
    finally:
        if conn is not None:
            conn.close()


if __name__ == '__main__':
    logging.basicConfig(level=LOG_LEVEL)

    create_postal_districts_table()
    create_mrt_table()
    create_projects_table()
    create_transactions_table()
