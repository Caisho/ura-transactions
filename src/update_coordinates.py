import os
import requests
from dotenv import load_dotenv
import psycopg2 as pg

load_dotenv()
URA_ACCESS_KEY = os.getenv('URA_ACCESS_KEY')
URA_TOKEN_URL = os.getenv('URA_TOKEN_URL')
URA_PROPERTY_URL = os.getenv('URA_PROPERTY_URL')
ONEMAP_URL = os.getenv('ONEMAP_URL')
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


def get_coordinates(x=20276.794, y=31255.976):
    payload = {'X': x, 'Y': y}
    r = requests.get(
        url=ONEMAP_URL,
        params=payload)
    if r.status_code == requests.codes.ok:
        return r.json()
    return None


def get_projects_table():
    query = ('SELECT * FROM private_residential_property_projects'
             ' WHERE x IS NOT NULL')
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
    if projects:
        query = ('UPDATE private_residential_property_projects'
                 ' SET latitude = %s, longitude = %s'
                 ' WHERE project = %s'
                 ' AND street = %s'
                 ' AND x = %s'
                 ' AND y = %s')
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

                if None in [latitude, longitude]:
                    print(record)
                    wgs84_coordinates = get_coordinates(x, y)
                    if wgs84_coordinates:
                        latitude = wgs84_coordinates.get('latitude')
                        longitude = wgs84_coordinates.get('longitude')
                        t = (latitude, longitude, project, street, x, y)
                        cur.execute(query, t)
            conn.commit()
            cur.close()
        except (pg.Error) as e:
            print(e)
        finally:
            if conn is not None:
                conn.close()
