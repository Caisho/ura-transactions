import os
import json
from dotenv import load_dotenv
import psycopg2 as pg
import pandas as pd
from utils import get_coordinates_distance

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


def get_postal_districts_data():
    query = (
        'SELECT name as district, latitude, longitude '
        'FROM postal_districts;'
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


def get_transactions_data():
    query = (
        'SELECT *, EXTRACT(YEAR FROM contract_date)  as contract_year '
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


def get_transactions_mrt_data():
    query = (
        'SELECT prpt.*, EXTRACT(YEAR FROM contract_date) as contract_year, mrt_id, mrt_name, mrt_dist '
        'FROM private_residential_property_projects prpp '
        'INNER JOIN private_residential_property_transactions prpt '
        'ON prpp.project = prpt.project AND prpp.street = prpt.street;'
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


def get_area_type_labels():
    query = ('SELECT DISTINCT type_of_area FROM private_residential_property_transactions order by type_of_area asc')
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


def get_sale_type_labels():
    query = ('SELECT DISTINCT type_of_sale FROM private_residential_property_transactions order by type_of_sale asc')
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


def get_floor_range_labels():
    query = ('SELECT DISTINCT floor_range FROM private_residential_property_transactions order by floor_range asc')
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


def extract_postal_districts(path='./data/ura-postal-districts/ura-postal-districts-point.geojson'):
    """Extracts name, latitude and logitude data from geojson file

    Args:
        path (str): filepath to geojson file
    """
    query = (
        'INSERT INTO postal_districts ('
        '   name, latitude, longitude, postal_sector, location'
        ')'
        'VALUES'
        '    ('
        '        %s, %s, %s, %s, %s'
        '    ) ON CONFLICT DO NOTHING;'
        )
    conn = None
    try:
        conn = pg.connect(**params)
        cur = conn.cursor()

        with open(path) as f:
            features = json.load(f).get('features')
            for feature in features:
                name = 'D' + feature['properties']['name']
                longitude = feature['geometry']['coordinates'][0]
                latitude = feature['geometry']['coordinates'][1]
                postal = feature['properties']['postal-sector']
                location = feature['properties']['location']
                cur.execute(query, (name, latitude, longitude, postal, location))
            conn.commit()
    except (pg.Error) as e:
        print(e)
    finally:
        if conn:
            cur.close()
            conn.close()


def extract_mrt_coordinates(path='./data/mrt/rail-station-point.geojson'):
    """Extracts name, latitude and logitude data from geojson file

    Args:
        path (str): filepath to geojson file
    """
    query = (
        'INSERT INTO mrt ('
        '   name, longitude, latitude'
        ') '
        'VALUES'
        '    ('
        '        %s, %s, %s'
        '    ) ON CONFLICT DO NOTHING;'
        )
    conn = None
    try:
        conn = pg.connect(**params)
        cur = conn.cursor()

        with open(path) as f:
            features = json.load(f).get('features')
            for feature in features:
                name = feature['properties']['Name']
                geo_type = feature['geometry']['type']
                if geo_type == 'Point':
                    longitude = feature['geometry']['coordinates'][0]
                    latitude = feature['geometry']['coordinates'][1]
                else:
                    longitude = feature['geometry']['coordinates'][0][0]
                    latitude = feature['geometry']['coordinates'][0][1]
                cur.execute(query, (name, longitude, latitude))
            conn.commit()
    except (pg.Error) as e:
        print(e)
    finally:
        if conn:
            cur.close()
            conn.close()


def update_proj_mrt_coordinates():
    proj_query = (
        'SELECT project, street, longitude, latitude, mrt_id, mrt_name, mrt_dist '
        'FROM private_residential_property_projects;'
        )
    mrt_query = (
        'SELECT id, name, longitude, latitude '
        'FROM mrt;'
    )
    insert_query = (
        'UPDATE private_residential_property_projects '
        'SET mrt_id = %s, mrt_name = %s, mrt_dist = %s '
        'WHERE project = %s AND street = %s'
    )
    conn = None
    try:
        conn = pg.connect(**params)
        cur = conn.cursor()
        cur.execute(proj_query)
        proj_records = cur.fetchall()
        cur.execute(mrt_query)
        mrt_records = cur.fetchall()

        for proj in proj_records:
            proj_name = proj[0]
            proj_street = proj[1]
            proj_long = float(proj[2])
            proj_lat = float(proj[3])
            mrt_id = proj[4]
            mrt_name = proj[5]
            mrt_dist = proj[6]
            proj_coord = (proj_long, proj_lat)

            if None in (mrt_id, mrt_name):
                closest_mrt_id = None
                closest_mrt_name = None
                closest_mrt_dist = 9999999
                for mrt in mrt_records:
                    mrt_long = float(mrt[2])
                    mrt_lat = float(mrt[3])
                    mrt_coord = (mrt_long, mrt_lat)
                    dist = get_coordinates_distance(proj_coord, mrt_coord)
                    if dist < closest_mrt_dist:
                        closest_mrt_id = mrt[0]
                        closest_mrt_name = mrt[1]
                        closest_mrt_dist = dist
                print(proj[0], closest_mrt_id, closest_mrt_name, closest_mrt_dist)
                cur.execute(insert_query, (closest_mrt_id, closest_mrt_name, closest_mrt_dist, proj_name, proj_street))
        conn.commit()
    except (pg.Error) as e:
        print(e)
    finally:
        if conn:
            cur.close()
            conn.close()
