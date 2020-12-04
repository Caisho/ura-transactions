## Introduction


## Installation
Git clone ura-transactions repository
```bash
git clone https://github.com/Caisho/ura-transactions.git
```
### Setup virtual env 

Setup conda or python virtual environment to install packages.

```bash
cd ura-transactions
# if using conda env
conda env create -f environment.yml
pip install -r requirements.txt
# if using python venv
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

```

### URA access key
You can register for an account [here](https://www.ura.gov.sg/maps/api/reg.html). After activation of your account, you will receive an email with an access key from which you can generate a token for access to the API. The email will also contain a user manual on URA Data Service API in PDF format.

### Mapbox token 
To get a token for yourself, create an account at https://mapbox.com. It’s free! (for moderate usage levels) See https://docs.streamlit.io/en/latest/cli.html#view-all-config-options for more info on how to set config options.


### Setup Env file
Create `.env` in root directory with the key-value pairs below. Friendly reminder to get your own URA Access Key and Mapbox API token. 
```
# Logging
LOG_LEVEL=INFO

# URA 
URA_TOKEN_URL=https://www.ura.gov.sg/uraDataService/insertNewToken.action
URA_PROPERTY_URL=https://www.ura.gov.sg/uraDataService/invokeUraDS
URA_ACCESS_KEY=my-access-key

# OneMap
ONEMAP_COORD_URL=https://developers.onemap.sg/commonapi/convert/3414to4326
ONEMAP_SEARCH_URL=https://developers.onemap.sg/commonapi/search

# Postgres
POSTGRES_DB=postgres
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_HOST=localhost
POSTGRES_PORT=5432

# Mapbox
MAPBOX_STYLE=mapbox://styles/caisho/ckhzpiwfm1x7419pujepchs2x
MAPBOX_API_KEY=my-api-key
```

### Get geojson data files

Public Data location

## Running

```bash
cd ura-transactions
docker-compose up -d
python migrate.py
python streamlit run src/app.py
```

## More Information


