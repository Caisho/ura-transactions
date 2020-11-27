import streamlit as st
import pydeck as pdk

MAPBOX_STYLE = 'mapbox://styles/caisho/ckhzpiwfm1x7419pujepchs2x'

st.sidebar.markdown('### Map Layers')

st.pydeck_chart(pdk.Deck(
    map_style=MAPBOX_STYLE,
    initial_view_state=pdk.ViewState(
        latitude=1.2882,
        longitude=103.7988,
        zoom=9.8,
        pitch=5,
    )
))