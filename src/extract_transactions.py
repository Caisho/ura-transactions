import os
import math
import requests
import logging
from dotenv import load_dotenv
import psycopg2 as pg
from update_coordinates import update_project_coordinates
from postgres_utils import update_proj_mrt_coordinates, extract_mrt_coordinates, extract_postal_districts
from utils import get_tenure_type, convert_abbreviation, format_date

LOGGER = logging.getLogger(__name__)

load_dotenv()
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
URA_ACCESS_KEY = os.getenv('URA_ACCESS_KEY')
URA_TOKEN_URL = os.getenv('URA_TOKEN_URL', 'https://www.ura.gov.sg/uraDataService/insertNewToken.action')
URA_PROPERTY_URL = os.getenv('URA_PROPERTY_URL', 'https://www.ura.gov.sg/uraDataService/invokeUraDS')
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
    proj_query = (
        'INSERT INTO private_residential_property_projects (project, street, x, y)'
        'VALUES'
        '    (%s, %s, %s, %s) ON CONFLICT DO NOTHING;'
    )
    trans_query = (
        'INSERT INTO private_residential_property_transactions ('
        '    project, street, area, floor_range,'
        '    no_of_units, contract_date, type_of_sale,'
        '    price, property_type, district, type_of_area,'
        '    tenure, psf, tenure_type'
        ')'
        'VALUES'
        '    ('
        '        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s'
        '    ) ON CONFLICT DO NOTHING;'
    )
    conn = None
    try:
        conn = pg.connect(**params)
        cur = conn.cursor()
        LOGGER.info(f'Total projects in batch = {len(data)}')
        trans_count = 0
        for project in data:
            street = convert_abbreviation(project.get('street'))
            project_name = project.get('project')
            x = project.get('x')
            y = project.get('y')
            cur.execute(proj_query, (project_name, street, x, y))

            transactions = project.get('transaction')
            if transactions is None:
                LOGGER.warning(f'No transactions found for {project}')
            else:
                trans_count += len(transactions)
                for transaction in transactions:
                    area = math.floor(float(transaction.get('area')) * 10.764)
                    floor_range = transaction.get('floorRange')
                    no_of_units = int(transaction.get('noOfUnits'))
                    contract_date = format_date(transaction.get('contractDate'))
                    type_of_sale = transaction.get('typeOfSale')
                    price = float(transaction.get('price'))
                    property_type = transaction.get('propertyType')
                    district = 'D' + transaction.get('district')
                    type_of_area = transaction.get('typeOfArea')
                    tenure = transaction.get('tenure')
                    psf = math.floor(price / area)
                    tenure_type = get_tenure_type(tenure)
                    values = (project_name, street, area, floor_range, no_of_units, contract_date, type_of_sale, price, property_type, district, type_of_area, tenure, psf, tenure_type)
                    cur.execute(trans_query, values)
        LOGGER.info(f'Total transactions in batch = {trans_count}')
        conn.commit()
    except (pg.DatabaseError) as e:
        LOGGER.exception(e)
    finally:
        if conn:
            cur.close()
            conn.close()


if __name__ == '__main__':
    logging.basicConfig(level=LOG_LEVEL)

    token = get_token(URA_TOKEN_URL, URA_ACCESS_KEY)
    extract_postal_districts()
    extract_mrt_coordinates()
    for i in range(1, 5, 1):
        LOGGER.info(f'Batch = {i}')
        result = get_private_residential_transactions(URA_PROPERTY_URL, URA_ACCESS_KEY, token, batch=i)
        extract_transactions(result)
    LOGGER.info('Updating projects longitude and latitude')
    update_project_coordinates()
    LOGGER.info('Updating projects nearest mrt')
    update_proj_mrt_coordinates()
