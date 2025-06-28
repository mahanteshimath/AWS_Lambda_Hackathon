import streamlit as st
import pandas as pd
import requests
import plotly.express as px
import snowflake.connector
from datetime import datetime
import pytz
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

# Visualization
st.title(":blue[ India LIVE AQI Dashboard 🌍]")
st.subheader(":blue[Real-time Air Quality Index (AQI) across Indian states.]")



if st.button("Fetch and push latest AQI Data to snowflake"):

    Q1=f'''SELECT * FROM IND_DB.IND_SCH.WEATHER_DATA'''
    R1 = execute_query(Q1)
    r1_expander = st.expander("Data sets used in this entire analysis.")
    R1_DF = pd.DataFrame(R1)
    R1_DF.index = R1_DF.index + 1
    r1_expander.write(R1_DF)
    df=R1_DF
    # --- Enhanced Visualization Section ---
    st.subheader(":blue[City-wise AQI Overview]")
    if not df.empty:
        # Show a summary table
        st.dataframe(df.head(20), use_container_width=True)

        # Show AQI by city as a bar chart (if 'CITY' and 'AQI' columns exist)
        if 'CITY' in df.columns and 'AQI' in df.columns:
            st.plotly_chart(
                px.bar(
                    df.sort_values('AQI', ascending=False),
                    x='CITY', y='AQI', color='AQI',
                    color_continuous_scale='RdYlGn_r',
                    title='AQI by City',
                    labels={'AQI': 'Air Quality Index', 'CITY': 'City'}
                ),
                use_container_width=True
            )

        # Show AQI trend over time by city (if 'INSRT_TIMESTAMP' and 'AQI' columns exist)
        if 'INSRT_TIMESTAMP' in df.columns and 'AQI' in df.columns:
            st.plotly_chart(
                px.line(
                    df.sort_values('INSRT_TIMESTAMP'),
                    x='INSRT_TIMESTAMP', y='AQI', color='CITY',
                    title='AQI Trend Over Time by City',
                    labels={'INSRT_TIMESTAMP': 'Timestamp', 'AQI': 'Air Quality Index'}
                ),
                use_container_width=True
            )
    else:
        st.info('No data available to visualize.')

# # Custom CSS
# st.markdown("""
#     <style>
#     .metric-container {
#         display: flex;
#         gap: 1rem;
#         justify-content: space-around;
#         margin: 1rem 0;
#     }
#     .metric-box {
#         background-color: black;
#         border: 3px solid #ddd;
#         border-radius: 8px;
#         padding: 10px;
#         text-align: center;
#         width: 150px;
#         box-shadow: 0 0 10px rgba(255, 255, 255, 0.5); /* Glow effect */
#         transition: box-shadow 0.3s ease-in-out;
#     }
#     .metric-text {
#         color: white; /* White text */
#         font-weight: bold; /* Bold text */
#     }
#     </style>
# """, unsafe_allow_html=True)

# # Default Values

# default_state = "Delhi"
# default_city = "New Delhi"

# # Sidebar Filters
# state_filter = st.selectbox("Select CITY", df["CITY"].unique(), index=df["CITY"].unique().tolist().index(default_city))

# # Filter Data
# filtered_data = df[(df["CITY"] == state_filter)]


# if not filtered_data.empty:
#     # Metrics Section
#     latest_entry = filtered_data.iloc[-1]
#     st.subheader(f"Air Quality Metrics for  {state_filter} as of :  {latest_entry['INSRT_TIMESTAMP']}")
    
#     # metric_html = f"""
#     #     <div class="metric-container">
#     #         <div class="metric-box"><strong>PM2.5</strong><br>{latest_entry['PM25']}<br><small>{latest_entry['PM25_CATEGORY']}</small></div>
#     #         <div class="metric-box"><strong>PM10</strong><br>{latest_entry['PM10']}</div>
#     #         <div class="metric-box"><strong>US EPA Index</strong><br>{latest_entry['US_EPA_INDEX']}</div>
#     #         <div class="metric-box"><strong>CO</strong><br>{latest_entry['CO']}</div>
#     #         <div class="metric-box"><strong>O3</strong><br>{latest_entry['O3']}</div>
#     #         <div class="metric-box"><strong>NO2</strong><br>{latest_entry['NO2']}</div>
#     #         <div class="metric-box"><strong>SO2</strong><br>{latest_entry['SO2']}</div>
#     #     </div>
#     # """
#     # st.markdown(metric_html, unsafe_allow_html=True)
#     us_epa_index_descriptions = {
#     1: 'Good',
#     2: 'Moderate',
#     3: 'Unhealthy for sensitive group',
#     4: 'Unhealthy',
#     5: 'Very Unhealthy',
#     6: 'Hazardous'
#     }

#     metric_html = f"""
#     <div class="metric-container">
#         <div class="metric-box"><strong>PM2.5</strong><br>{latest_entry['PM25']}<br><small>{latest_entry['PM25_CATEGORY']}</small></div>
#         <div class="metric-box"><strong>PM10</strong><br>{latest_entry['PM10']}</div>
#         <div class="metric-box"><strong>US EPA Index</strong><br>{latest_entry['US_EPA_INDEX']}<br><small>{us_epa_index_descriptions.get(latest_entry['US_EPA_INDEX'], 'Unknown')}</small></div>
#         <div class="metric-box"><strong>CO</strong><br>{latest_entry['CO']}</div>
#         <div class="metric-box"><strong>O3</strong><br>{latest_entry['O3']}</div>
#         <div class="metric-box"><strong>NO2</strong><br>{latest_entry['NO2']}</div>
#         <div class="metric-box"><strong>SO2</strong><br>{latest_entry['SO2']}</div>
#         <div class="metric-box"><strong>AQI</strong><br>{latest_entry['AQI']}</div>
#         <div class="metric-box"><strong>AIR_QUALITY</strong><br>{latest_entry['AIR_QUALITY']}</div>
#     </div>
#     """
#     st.markdown(metric_html, unsafe_allow_html=True)
    
#     # Historical Data Table
#     st.write("### Historical Data")
#     st.dataframe(filtered_data)
    
#     # Visualizations
#     st.write("### Trend Air Quality Data Over Time")
#     # st.line_chart(filtered_data.set_index("INSRT_TIMESTAMP")[["PM25", "PM10", "CO", "O3", "NO2", "SO2"]])

#     # Create a line chart with Plotly
#     fig = px.line(filtered_data, x='INSRT_TIMESTAMP', y=["PM25", "PM10", "CO", "O3", "NO2", "SO2"],
#                 labels={"INSRT_TIMESTAMP": "Timestamp", "value": "Concentration (µg/m³)"},
#                 title="Air Quality Data Over Time", markers=True)

#     # Display the plot
#     st.plotly_chart(fig, use_container_width=True)

#     fig_aqi = px.line(filtered_data, x='INSRT_TIMESTAMP', y=["AQI"],
#                 labels={"INSRT_TIMESTAMP": "Timestamp", "value": "AQI"},
#                 title="Trend AQI  Over Time", markers=True)

#     # Display the plot
#     st.plotly_chart(fig_aqi, use_container_width=True)
# else:
#     st.warning("No data available for the selected filters.")

st.header("💡 Recommendations")
st.image("./src/AQI.jpeg", caption="10 AI-specific ways to reduce air pollution in Delhi")
st.markdown("""
Here are **10 AI-specific ways to reduce air pollution in Delhi**, tailored to its challenges:

1. **Real-time Air Quality Forecasting**  
   AI can analyze data from Delhi's air quality sensors (like SAFAR) to predict pollution levels and issue health advisories. This allows residents to plan activities during low-pollution hours and helps policymakers enforce short-term measures.

2. **Traffic Congestion Reduction**  
   AI can optimize Delhi's traffic flow by integrating data from ITS (Intelligent Transport Systems), cameras, and GPS. Dynamic signal adjustments, congestion alerts, and promoting alternate routes can reduce vehicular emissions, a significant contributor to Delhi's air pollution.

3. **Reducing Stubble Burning Impact**  
   AI models can predict wind patterns and the effect of stubble-burning smoke from neighboring states on Delhi’s air quality. This can help authorities coordinate preventive measures like water sprinkling and cloud seeding in advance.

4. **Identifying High-Emission Vehicles**  
   Using AI-powered cameras at major checkpoints (like Delhi-Gurugram or Delhi-Noida borders), authorities can monitor and restrict entry of non-compliant vehicles (e.g., older or diesel vehicles) to reduce tailpipe emissions.

5. **Industrial Pollution Monitoring**  
   AI can track emissions from factories in Delhi and surrounding NCR regions using IoT sensors and satellite data. Non-compliance alerts can help authorities enforce emission norms and shut down illegal polluting units.

6. **Optimizing Waste Management**  
   AI systems can streamline waste collection and disposal in Delhi by identifying hotspots for illegal garbage burning and recommending efficient collection routes, reducing harmful emissions.

7. **Promoting Green Cover**  
   AI can help identify regions in Delhi where planting trees will have the maximum impact on air quality. Machine learning models can predict the effectiveness of different tree species in absorbing pollutants like PM2.5 and PM10.

8. **Monitoring and Reducing Construction Dust**  
   AI-powered drones can monitor construction sites in Delhi to ensure compliance with dust control measures (e.g., covering materials, using water sprinklers). Automated alerts can penalize violators, reducing PM levels.

9. **Public Awareness Campaigns**  
   AI chatbots and mobile apps can provide Delhi residents with tailored pollution reduction tips, such as using public transport, carpooling, and avoiding wood or coal-burning heaters in winters.

10. **Cleaner Energy for Public Transport**  
    AI can optimize the deployment of Delhi’s electric buses by analyzing commuter patterns and identifying routes where replacing diesel buses with electric ones will have the most significant environmental impact.

### Immediate AI-driven Initiatives:  
- Integrate AI with the **Delhi AQI Dashboard** for predictive insights.  
- Deploy AI cameras for enforcing **GRAP (Graded Response Action Plan)** measures.  
- Use satellite monitoring to track real-time stubble-burning impacts.  

These measures can directly address Delhi's primary pollution sources, like vehicular emissions, construction dust, and stubble burning, to make the city more livable.

""")

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

