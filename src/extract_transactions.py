import os
import requests
import logging
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
URA_ACCESS_KEY = os.getenv('URA_ACCESS_KEY')
URA_TOKEN_URL = os.getenv('URA_TOKEN_URL')
URA_PROPERTY_URL = os.getenv('URA_PROPERTY_URL')


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
    street = data.get('street')
    project = data.get('project')
    x = data.get('x')
    y = data.get('y')
    transaction = data.get('transaction')
    if transaction is not None:
        for t in transaction:
            area = t.get('area')
            floor_range = t.get('floorRange')
            no_of_units = t.get('noOfUnits')
            contract_date = t.get('contractDate')
            type_of_sale = t.get('typeOfSale')
            price = t.get('price')
            property_type = t.get('propertyType')
            district = t.get('district')
            type_of_area = t.get('typeOfArea')
            tenure = t.get('tenure')


token = get_token(URA_TOKEN_URL, URA_ACCESS_KEY)
r = get_private_residential_transactions(URA_PROPERTY_URL, URA_ACCESS_KEY, token)

df = pd.read_json(r)
