import streamlit as st
import pydeck as pdk
import pandas as pd
from postgres_utils import get_property_type_labels, get_contract_date_years, get_tenure_type_labels, get_area_type_labels, get_transactions_data, get_postal_districts_data

MAPBOX_STYLE = 'mapbox://styles/caisho/ckhzpiwfm1x7419pujepchs2x'
AREA_TYPES = get_area_type_labels()
PROPERTY_TYPES = get_property_type_labels()
TRANSACTION_YEARS = get_contract_date_years()
TENURE_TYPES = get_tenure_type_labels()


@st.cache
def get_postgres_transactions_data():
    return get_transactions_data()


@st.cache
def get_postgres_districts_data():
    return get_postal_districts_data()


# Sidebar
st.sidebar.subheader('Map Layers')
start_year = st.sidebar.selectbox(
    label='Start year of transactions',
    options=TRANSACTION_YEARS,
)

end_year = st.sidebar.selectbox(
    label='End year of transactions',
    options=TRANSACTION_YEARS,
    index=5
)

min_area = st.sidebar.number_input('Min area (sqft)', value=0)
max_area = st.sidebar.number_input('Max area (sqft)', value=999999)

area_type = st.sidebar.multiselect(
    label='Select area type(s)',
    options=AREA_TYPES,
    default=AREA_TYPES,
)

property_type = st.sidebar.multiselect(
    label='Select property type(s)',
    options=PROPERTY_TYPES,
    default=PROPERTY_TYPES,
)

tenure_type = st.sidebar.multiselect(
    label='Select tenure type(s)',
    options=TENURE_TYPES,
    default=TENURE_TYPES,
)

# Body
st.title('URA Private Residential Property Transactions')

df = get_postgres_transactions_data()
df_filtered = df.loc[
    (df['contract_year'] >= start_year) &
    (df['contract_year'] <= end_year) &
    (df['area'] >= min_area) &
    (df['area'] <= max_area) &
    (df['type_of_area'].isin(area_type)) &
    (df['property_type'].isin(property_type)) &
    (df['tenure_type'].isin(tenure_type))]

st.subheader('Individual Transactions')
st.write(df_filtered)
st.write(f'Total Transactions: {len(df_filtered)}')

st.subheader('District Transactions')
df_group = df_filtered.groupby(['district']).mean()
st.write(df_group)

df_districts = get_postgres_districts_data()
df_districts = df_districts.set_index('district')
df_map = df_districts.join(df_group, how='inner', on='district')
df_map['psf'] = df_map['psf'].to_numpy().astype('int').astype('str')  # hack to get it to work
df_map['psf'] = df_map['psf'] + 'psf'

df_map['latitude'] = df_map['latitude'] - 0.002

st.subheader('Average PSF by District')
st.pydeck_chart(pdk.Deck(
    map_style=MAPBOX_STYLE,
    initial_view_state=pdk.ViewState(
        latitude=1.2882,
        longitude=103.7988,
        zoom=9.8,
        pitch=5,
    ),
    layers=[
        pdk.Layer(
            'TextLayer',
            data=df_map,
            get_position='[longitude, latitude]',  # must be numeric
            get_text='psf',  # must be varchar
            get_color=[200, 30, 0, 160],
            get_size=20,
            get_alignment_baseline="'top'",
        ),
    ],
))