import streamlit as st
import pydeck as pdk
import pandas as pd
from postgres_utils import get_property_type_labels, get_contract_date_years, get_tenure_type_labels, get_transactions_data

MAPBOX_STYLE = 'mapbox://styles/caisho/ckhzpiwfm1x7419pujepchs2x'
PROPERTY_TYPES = get_property_type_labels()
TRANSACTION_YEARS = get_contract_date_years()
TENURE_TYPES = get_tenure_type_labels()


@st.cache
def get_data():
    return get_transactions_data().head()


# Sidebar
st.sidebar.subheader('Map Layers')
start_date = st.sidebar.selectbox(
    label='Start date of transactions',
    options=TRANSACTION_YEARS,
)

end_date = st.sidebar.selectbox(
    label='End date of transactions',
    options=TRANSACTION_YEARS,
    index=3
)

property_type = st.sidebar.multiselect(
    label='Select property type(s)',
    options=PROPERTY_TYPES,
    default=PROPERTY_TYPES,
)

tenure_type = st.sidebar.multiselect(
    label='Select property type(s)',
    options=TENURE_TYPES,
    default=TENURE_TYPES,
)
# area 
# type of sale
# type of area (Land, Strata)


# Body
st.title('Singapore District Map')

df = get_data()
st.write(df.head())

st.pydeck_chart(pdk.Deck(
    map_style=MAPBOX_STYLE,
    initial_view_state=pdk.ViewState(
        latitude=1.2882,
        longitude=103.7988,
        zoom=9.8,
        pitch=5,
    )
))