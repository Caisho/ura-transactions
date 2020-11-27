import streamlit as st
import pydeck as pdk

MAPBOX_STYLE = 'mapbox://styles/caisho/ckhzpiwfm1x7419pujepchs2x'
PROPERTY_TYPES = [
    'Apartment',
    'Condominium',
    'Detached',
    'Executive Condominium',
    'Semi-detached',
    'Strata Detached',
    'Strata Semi-detached',
    'Strata Terrace',
    'Terrace',
]

# Sidebar
st.sidebar.subheader('Map Layers')
start_date = st.sidebar.selectbox(
    label='Start date of transactions',
    options=['2017', '2018', '2019', '2020']
)

end_date = st.sidebar.selectbox(
    label='End date of transactions',
    options=['2017', '2018', '2019', '2020'],
    index=3
)

property_type = st.sidebar.multiselect(
    label='Select property type(s)',
    options=PROPERTY_TYPES,
    default=PROPERTY_TYPES,
)

# area 
# type of sale
# tenure
# type of area (Land, Strata)


# Body
st.title('Singapore District Map')
st.text('Hello Joni')

st.pydeck_chart(pdk.Deck(
    map_style=MAPBOX_STYLE,
    initial_view_state=pdk.ViewState(
        latitude=1.2882,
        longitude=103.7988,
        zoom=9.8,
        pitch=5,
    )
))