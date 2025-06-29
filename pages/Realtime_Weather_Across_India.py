import streamlit as st
import pandas as pd
import requests
import plotly.express as px
import snowflake.connector
from datetime import datetime
import pytz
import boto3
ist_timezone = pytz.timezone('Asia/Kolkata')
current_time_ist = datetime.now(ist_timezone)
current_time_ist = current_time_ist.strftime("%Y-%m-%d %H:%M:%S")
col1,col2 = st.columns(2)
with col1:
    st.image("./src/DH1.PNG", caption="What we like", use_column_width=True)
with col2:
    st.image("./src/DH2.PNG", caption="This is Now", use_column_width=True)

def execute_query(query):
    try:
        conn = snowflake.connector.connect(
            account=st.session_state.account,
            role=st.session_state.role,
            warehouse=st.session_state.warehouse,
            database=st.session_state.database,
            schema=st.session_state.schema,
            user=st.session_state.user,
            password=st.session_state.password,
            client_session_keep_alive=True

        )
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        columns = [col[0] for col in cursor.description] 
        conn.close()
        result_df = pd.DataFrame(result, columns=columns)
        return result_df
    except Exception as e:
        st.error(f"Error executing query: {str(e)}")
        return None

# Visualizatio

st.title(":blue[ Indias realtime weather insights in major cities 🌍]")
st.subheader(":blue[Real-time Weather Data across Indian states.]")

# Add this before the button to initialize the session state flag
if 'weather_data_loaded' not in st.session_state:
    st.session_state.weather_data_loaded = False

if st.button("Trigger AWS Lambda to push data to Snowflake"):
    # Trigger AWS Lambda function
    try:
        lambda_client = boto3.client(
            'lambda',
            region_name=st.session_state.region_name,
            aws_access_key_id=st.session_state.aws_access_key_id,
            aws_secret_access_key=st.session_state.aws_secret_access_key
        )
        response = lambda_client.invoke(
            FunctionName='snowstream',
            InvocationType='RequestResponse'
        )
        st.success("AWS Lambda function 'snowstream' triggered successfully!")
    except Exception as e:
        st.error(f"Failed to trigger AWS Lambda: {e}")
    st.session_state.weather_data_loaded = True

if st.session_state.weather_data_loaded:
    Q1=f'''SELECT * FROM IND_DB.IND_SCH.WEATHER_DATA'''
    R1 = execute_query(Q1)
    r1_expander = st.expander("Data sets used in this entire analysis.")
    R1_DF = pd.DataFrame(R1)
    R1_DF.index = R1_DF.index + 1
    r1_expander.write(R1_DF)
    df = R1_DF  # Use R1_DF everywhere for visualization
    # --- Enhanced Visualization Section ---
    st.subheader(":blue[Location-wise Weather Overview]")
    if isinstance(df, pd.DataFrame) and not df.empty:
        # Location filter
        if 'LOCATION_NAME' in df.columns:
            locations = df['LOCATION_NAME'].dropna().unique().tolist()
            selected_location = st.selectbox('Select Location', ['All'] + sorted(locations))
            if selected_location != 'All':
                filtered_df = df[df['LOCATION_NAME'] == selected_location]
            else:
                filtered_df = df
        else:
            filtered_df = df
        st.dataframe(filtered_df.head(20), use_container_width=True)
        # Temperature by Location
        if 'LOCATION_NAME' in filtered_df.columns and 'TEMP_C' in filtered_df.columns:
            st.plotly_chart(
                px.bar(
                    filtered_df.sort_values('TEMP_C', ascending=False),
                    x='LOCATION_NAME', y='TEMP_C', color='TEMP_C',
                    color_continuous_scale='Bluered',
                    title=f"Temperature (°C) by Location{' for ' + selected_location if selected_location != 'All' else ''}",
                    labels={'TEMP_C': 'Temperature (°C)', 'LOCATION_NAME': 'Location'}
                ),
                use_container_width=True
            )
        # Humidity by Location
        if 'LOCATION_NAME' in filtered_df.columns and 'HUMIDITY' in filtered_df.columns:
            st.plotly_chart(
                px.bar(
                    filtered_df.sort_values('HUMIDITY', ascending=False),
                    x='LOCATION_NAME', y='HUMIDITY', color='HUMIDITY',
                    color_continuous_scale='Viridis',
                    title=f"Humidity (%) by Location{' for ' + selected_location if selected_location != 'All' else ''}",
                    labels={'HUMIDITY': 'Humidity (%)', 'LOCATION_NAME': 'Location'}
                ),
                use_container_width=True
            )
        # Wind Speed by Location
        if 'LOCATION_NAME' in filtered_df.columns and 'WIND_KPH' in filtered_df.columns:
            st.plotly_chart(
                px.bar(
                    filtered_df.sort_values('WIND_KPH', ascending=False),
                    x='LOCATION_NAME', y='WIND_KPH', color='WIND_KPH',
                    color_continuous_scale='Cividis',
                    title=f"Wind Speed (kph) by Location{' for ' + selected_location if selected_location != 'All' else ''}",
                    labels={'WIND_KPH': 'Wind Speed (kph)', 'LOCATION_NAME': 'Location'}
                ),
                use_container_width=True
            )
        # Temperature trend over time (if time column exists)
        if 'LOCALTIME_STR' in filtered_df.columns and 'TEMP_C' in filtered_df.columns and 'LOCATION_NAME' in filtered_df.columns:
            st.plotly_chart(
                px.line(
                    filtered_df.sort_values('LOCALTIME_STR'),
                    x='LOCALTIME_STR', y='TEMP_C', color='LOCATION_NAME',
                    title=f"Temperature Trend Over Time{' for ' + selected_location if selected_location != 'All' else ''}",
                    labels={'LOCALTIME_STR': 'Local Time', 'TEMP_C': 'Temperature (°C)'}
                ),
                use_container_width=True
            )
    else:
        st.info('No data available to visualize.')
    st.success("Data fetched and pushed to Snowflake successfully!")


st.markdown(
    '''
    <style>
    .streamlit-expanderHeader {
        background-color: blue;
        color: white; # Adjust this for expander header color
    }
    .streamlit-expanderContent {
        background-color: blue;
        color: white; # Expander content color
    }
    </style>
    ''',
    unsafe_allow_html=True
)

footer="""<style>

.footer {
position: fixed;
left: 0;
bottom: 0;
width: 100%;
background-color: #2C1E5B;
color: white;
text-align: center;
}
</style>
<div class="footer">
<p>Developed with ❤️ by <a style='display: inline; text-align: center;' href="https://www.linkedin.com/in/mahantesh-hiremath/" target="_blank">MAHANTESH HIREMATH</a></p>
</div>
"""
st.markdown(footer,unsafe_allow_html=True)

