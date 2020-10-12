import os
import math
import requests
import datetime
from dotenv import load_dotenv
import psycopg2 as pg

load_dotenv()
URA_ACCESS_KEY = os.getenv('URA_ACCESS_KEY')
URA_TOKEN_URL = os.getenv('URA_TOKEN_URL')
URA_PROPERTY_URL = os.getenv('URA_PROPERTY_URL')
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


def get_token(url, access_key):
    headers = {'AccessKey': access_key}
    r = requests.get(
        url=url,
        headers=headers)
    if r.status_code == requests.codes.ok:
        return r.json().get('Result')
    return None


def get_private_residential_transactions(url, access_key, token, batch=1):
    headers = {
        'AccessKey': access_key,
        'Token': token}
    payload = {
        'service': 'PMI_Resi_Transaction',
        'batch': batch
    }
    r = requests.get(
        url=url,
        headers=headers,
        params=payload)
    if r.status_code == requests.codes.ok:
        return r.json().get('Result')
    return None


def extract_transactions(data):
    query = ('INSERT INTO private_residential_property_transactions'
             ' (project, street, x, y, area, floor_range, no_of_units, contract_date, type_of_sale, price, property_type, district, type_of_area, tenure, psf)'
             ' VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
             ' ON CONFLICT DO NOTHING;')
    conn = None
    try:
        conn = pg.connect(**params)
        cur = conn.cursor()
        for project in data:
            street = project.get('street')
            project_name = project.get('project')
            x = project.get('x')
            y = project.get('y')
            transactions = project.get('transaction')
            if transactions is not None:
                for transaction in transactions:
                    area = math.floor(float(transaction.get('area')) * 10.764)
                    floor_range = transaction.get('floorRange')
                    no_of_units = transaction.get('noOfUnits')
                    contract_date = format_date(transaction.get('contractDate'))
                    type_of_sale = transaction.get('typeOfSale')
                    price = float(transaction.get('price'))
                    property_type = transaction.get('propertyType')
                    district = transaction.get('district')
                    type_of_area = transaction.get('typeOfArea')
                    tenure = transaction.get('tenure')
                    psf = math.floor(price / area)
                    t = (project_name, street, x, y, area, floor_range, no_of_units, contract_date, type_of_sale, price, property_type, district, type_of_area, tenure, psf)
                    cur.execute(query, t)
        conn.commit()
        cur.close()
    except (pg.DatabaseError) as e:
        print(e)
    finally:
        if conn is not None:
            conn.close()


def format_date(dt_str):
    dt = datetime.datetime.strptime(dt_str, '%m%y')
    return dt


if __name__ == '__main__':
    token = get_token(URA_TOKEN_URL, URA_ACCESS_KEY)
    for i in range(1, 5, 1):
        print(f'batch = {i}')
        result = get_private_residential_transactions(URA_PROPERTY_URL, URA_ACCESS_KEY, token, batch=i)
        print(f'total projects = {len(result)}')
        extract_transactions(result)
