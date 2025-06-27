import streamlit as st
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from pathlib import Path
import time
import pandas as pd
from PIL import Image
from io import BytesIO
import requests 

st.logo(
    image="https://upload.wikimedia.org/wikipedia/en/4/41/Flag_of_India.svg",
    link="https://www.linkedin.com/in/mahantesh-hiremath/",
    icon_image="https://upload.wikimedia.org/wikipedia/en/4/41/Flag_of_India.svg"
)

st.set_page_config(
  page_title="WEATHER_INSIGHTS_APP",
  page_icon="🇮🇳",
  layout="wide",
  initial_sidebar_state="expanded",
) 


Architecture_page = st.Page(
    "pages/Architecture.py",
    title="Architecture",
    icon=":material/home:"
    default=True,

)

AQI = st.Page(
    "pages/Realtime_AQI_Across_India.py",
    title="Realtime AQI Across India",
    icon=":material/aq:",
)




pg = st.navigation(
    {
        "Info": [Architecture_page],
        "Weather insights": [AQI],
    }
)


pg.run()
