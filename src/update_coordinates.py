import os
import requests
from dotenv import load_dotenv
import psycopg2 as pg
from utils import get_tenure_type

load_dotenv()
URA_ACCESS_KEY = os.getenv('URA_ACCESS_KEY')
URA_TOKEN_URL = os.getenv('URA_TOKEN_URL')
URA_PROPERTY_URL = os.getenv('URA_PROPERTY_URL')
ONEMAP_COORD_URL = os.getenv('ONEMAP_COORD_URL')
ONEMAP_SEARCH_URL = os.getenv('ONEMAP_SEARCH_URL')
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


def get_xy_coordinates(x=20276.794, y=31255.976):
    payload = {'X': x, 'Y': y}
    r = requests.get(
        url=ONEMAP_COORD_URL,
        params=payload)
    if r.status_code == requests.codes.ok:
        return r.json()
    return None


def get_street_coordinates(street='ORCHARD BOULEVARD'):
    payload = {'searchVal': street, 'returnGeom': 'Y', 'getAddrDetails': 'N'}
    r = requests.get(
        url=ONEMAP_SEARCH_URL,
        params=payload)
    if r.status_code == requests.codes.ok:
        return r.json().get('results')
    return None


def get_projects_table():
    query = ('SELECT * FROM private_residential_property_projects')
    conn = None
    records = None
    try:
        conn = pg.connect(**params)
        cur = conn.cursor()
        cur.execute(query)
        records = cur.fetchall()
        cur.close()
    except (pg.Error) as e:
        print(e)
    finally:
        if conn is not None:
            conn.close()
        return records


def get_transactions_table():
    query = ('SELECT project, street, area, floor_range, contract_date, type_of_sale, price, tenure, tenure_type'
             ' FROM private_residential_property_transactions')
    conn = None
    records = None
    try:
        conn = pg.connect(**params)
        cur = conn.cursor()
        cur.execute(query)
        records = cur.fetchall()
        cur.close()
    except (pg.Error) as e:
        print(e)
    finally:
        if conn is not None:
            conn.close()
        return records


def update_project_coordinates():
    projects = get_projects_table()
    query1 = ('UPDATE private_residential_property_projects'
              ' SET x = %s, y = %s, latitude = %s, longitude = %s'
              ' WHERE project = %s'
              ' AND street = %s'
              ' AND x IS NULL'
              ' AND y IS NULL')

    query2 = ('UPDATE private_residential_property_projects'
              ' SET latitude = %s, longitude = %s'
              ' WHERE project = %s'
              ' AND street = %s'
              ' AND x = %s'
              ' AND y = %s')
    if projects:
        conn = None
        try:
            conn = pg.connect(**params)
            cur = conn.cursor()

            for record in projects:
                project = record[0]
                street = record[1]
                x = record[2]
                y = record[3]
                latitude = record[4]
                longitude = record[5]

                if None in [x, y]:
                    print(f'No XY found - {record}')
                    street_coord = get_street_coordinates(street)
                    if street_coord:
                        x = street_coord[0].get('X')
                        y = street_coord[0].get('Y')
                        latitude = street_coord[0].get('LATITUDE')
                        longitude = street_coord[0].get('LONGITUDE')
                        cur.execute(query1, (x, y, latitude, longitude, project, street))

                if None in [latitude, longitude]:
                    print(f'No long lat found - {record}')
                    wgs84_coord = get_xy_coordinates(x, y)
                    if wgs84_coord:
                        latitude = wgs84_coord.get('latitude')
                        longitude = wgs84_coord.get('longitude')
                        cur.execute(query2, (latitude, longitude, project, street, x, y))
            conn.commit()
        except (pg.Error) as e:
            print(e)
        finally:
            if conn is not None:
                conn.close()


def update_transactions_tenure():
    transactions = get_transactions_table()
    query = ('UPDATE private_residential_property_transactions'
             ' SET tenure_type = %s'
             ' WHERE project = %s'
             ' AND street = %s'
             ' AND area = %s'
             ' AND floor_range = %s'
             ' AND contract_date = %s'
             ' AND type_of_sale = %s'
             ' AND price = %s')
    conn = None
    try:
        conn = pg.connect(**params)
        cur = conn.cursor()

        for record in transactions:
            project = record[0]
            street = record[1]
            area = record[2]
            floor_range = record[3]
            contract_date = record[4]
            type_of_sale = record[5]
            price = record[6]
            tenure = record[7]
            tenure_type = record[8]

            if tenure_type is None:
                tenure_type = get_tenure_type(tenure)
                values = (tenure_type, project, street, area, floor_range, contract_date, type_of_sale, price)
                cur.execute(query, values)
        conn.commit()
    except (pg.Error) as e:
        print(e)
    finally:
        if conn is not None:
            conn.close()
