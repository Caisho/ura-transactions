import streamlit as st
import pydeck as pdk
import pandas as pd
import altair as alt
from postgres_utils import get_property_type_labels, get_contract_date_years, get_tenure_type_labels, get_area_type_labels, get_transactions_data, get_postal_districts_data, get_sale_type_labels, get_floor_range_labels, get_transactions_mrt_data, get_mrt_name_labels

MAPBOX_STYLE = 'mapbox://styles/caisho/ckhzpiwfm1x7419pujepchs2x'
AREA_TYPES = get_area_type_labels()
PROPERTY_TYPES = get_property_type_labels()
TRANSACTION_YEARS = get_contract_date_years()
TENURE_TYPES = get_tenure_type_labels()
SALE_TYPES = get_sale_type_labels()
FLOOR_RANGE_TYPES = get_floor_range_labels()
MRT_NAME_TYPES = get_mrt_name_labels()

@st.cache
def get_postgres_transactions_data():
    return get_transactions_mrt_data()


@st.cache
def get_postgres_districts_data():
    return get_postal_districts_data()


def aggregate_data(df, by):
    df_agg = df.groupby(by).agg(
        {'no_of_units': 'sum',
         'area': 'sum',
         'price': 'sum'
         })
    df_agg['area_mean'] = df_agg['area'] / df_agg['no_of_units']
    df_agg['price_mean'] = df_agg['price'] / df_agg['no_of_units']
    df_agg['psf_mean'] = df_agg['price'] / df_agg['area']
    return df_agg[['no_of_units', 'area_mean', 'price_mean', 'psf_mean']]


def compare_data(df, where, isin):
    df = df.reset_index()
    df_comp = df.loc[df[where].isin(isin)]
    return df_comp


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

sale_type = st.sidebar.multiselect(
    label='Select type of sale: [1: New Sale, 2: Sub Sale, 3: Resale]',
    options=SALE_TYPES,
    default=SALE_TYPES,
)

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

floor_range_type = st.sidebar.multiselect(
    label='Select floor range(s)',
    options=FLOOR_RANGE_TYPES,
    default=FLOOR_RANGE_TYPES,
)

mrt_name_type = st.sidebar.multiselect(
    label='Select MRT stations to compare',
    options=MRT_NAME_TYPES,
    default=['SIGLAP']
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
    (df['tenure_type'].isin(tenure_type)) &
    (df['floor_range'].isin(floor_range_type)) &
    (df['type_of_sale'].isin(sale_type))]

st.subheader('Individual Transactions')
st.write(df_filtered)
st.write(f'Total Transactions: {len(df_filtered)}')

st.subheader('Transactions by District')
df_district_grp = aggregate_data(df_filtered, ['district'])
st.write(df_district_grp)

st.subheader('Transactions by MRT')
df_mrt_grp = aggregate_data(df_filtered, ['mrt_name', 'contract_year'])
st.write(df_mrt_grp)

st.subheader('Average PSF by MRT and Year')
df_mrt_comp = compare_data(df_mrt_grp, 'mrt_name', mrt_name_type)
c = alt.Chart(df_mrt_comp).mark_bar().encode(
    x='contract_year:O',
    y='psf_mean:Q',
    color='contract_year:N',
    column='mrt_name:N'
)
st.altair_chart(c, use_container_width=False)


df_districts = get_postgres_districts_data()
df_districts = df_districts.set_index('district')
df_map = df_districts.join(df_district_grp, how='inner', on='district')
df_map['psf_mean'] = df_map['psf_mean'].to_numpy().astype('int').astype('str')  # hack to get it to work
df_map['psf_mean'] = df_map['psf_mean'] + 'psf'
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
            get_text='psf_mean',  # must be varchar
            get_color=[200, 30, 0, 160],
            get_size=20,
            get_alignment_baseline="'top'",
        ),
    ],
))