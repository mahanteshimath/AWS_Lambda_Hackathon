import streamlit as st
import streamlit.components.v1 as components

# Accessing the database credentials
db_credentials = st.secrets["db_credentials"]

if 'account' not in st.session_state:
    st.session_state.account = db_credentials["account"]
if 'role' not in st.session_state:
    st.session_state.role = db_credentials["role"]
if 'warehouse' not in st.session_state:
    st.session_state.warehouse = db_credentials["warehouse"]
if 'database' not in st.session_state:
    st.session_state.database = db_credentials["database"]
if 'schema' not in st.session_state:
    st.session_state.schema = db_credentials["schema"]
if 'user' not in st.session_state:
    st.session_state.user = db_credentials["user"]
if 'password' not in st.session_state:
    st.session_state.password = db_credentials["password"]
if 'region_name' not in st.session_state:
    st.session_state.region_name = db_credentials["region_name"]
if 'aws_access_key_id' not in st.session_state:
    st.session_state.aws_access_key_id = db_credentials["aws_access_key_id"]
if 'aws_secret_access_key' not in st.session_state:
    st.session_state.aws_secret_access_key = db_credentials["aws_secret_access_key"]

    

def store_credentials():
    st.session_state.account = db_credentials["account"]
    st.session_state.role = db_credentials["role"]
    st.session_state.warehouse = db_credentials["warehouse"]
    st.session_state.database = db_credentials["database"]
    st.session_state.schema = db_credentials["schema"]
    st.session_state.user = db_credentials["user"]
    st.session_state.password = db_credentials["password"]
    st.session_state.region_name = db_credentials["region_name"]
    st.session_state.aws_access_key_id = db_credentials["aws_access_key_id"]

def create_snowflake_connection():
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
        st.toast("Connection to Snowflake successfully!", icon='🎉')
        time.sleep(.5)
        st.balloons()
    except Exception as e:
        st.error(f"Error connecting to Snowflake: {str(e)}")
    return conn

# Load the HTML content from the file
with open("./src/Architectureembed.txt", "r") as file:
    html_content = file.read()


st.markdown("[⭐Please give star to motivate](https://github.com/mahanteshimath/AI_for_Good_SF/stargazers)", unsafe_allow_html=True)
# Display the HTML content as an embedded diagram in Streamlit
components.html(html_content, width=800, height=600)

st.image("./src/indian-major-cities-weather-insights.jpg", caption="Architecture Diagram", use_column_width=True)

st.markdown('''


## 📊 Title: **INDIAN-MAJOR-CITIES-WEATHER-INSIGHTS**

This solution is designed to provide weather insights for major Indian cities by fetching, storing, and visualizing the data.

---

## 🔄 Detailed Component Flow

### 1️⃣ **Weather App (External Weather API)**

* This is a third-party weather API (like OpenWeatherMap, WeatherStack, etc.)
* It provides weather data (temperature, humidity, wind speed, etc.) for Indian cities via HTTP endpoints (REST APIs).
* The system will call this API periodically (e.g., every hour or day) to get updated data.

---

### 2️⃣ **AWS Lambda (ETL Function)**

* This represents a serverless compute component (AWS Lambda).
* Responsibilities:

  * Fetch weather data from the external weather API.
  * Transform or clean the data if needed (e.g., convert units, handle missing data).
  * Prepare the data for loading into Snowflake.
* Benefits of using Lambda:

  * No need to manage servers.
  * Pay only for execution time.
  * Easy to trigger periodically using AWS CloudWatch Events.

---

### 📝 **Label: `Weather data fetched`**

* Indicates the data flow from the weather API to Lambda.
* Lambda is responsible for fetching this data.

---

### 3️⃣ **Snowflake DB (Data Warehouse)**

* A cloud-based data warehouse (Snowflake) is used to store weather data.

* Responsibilities:

  * Persist weather data for historical analysis.
  * Support querying, aggregation, and analytical workloads.

* Data is loaded into structured tables like:

  ```
  weather_data
  -----------------------------
LOCATION_NAME|LOCATION_REGION|LOCATION_COUNTRY|LOCATION_LAT|LOCATION_LON|LOCALTIME_EPOCH|LOCALTIME_STR|LAST_UPDATED_EPOCH|LAST_UPDATED_STR|TEMP_C|TEMP_F|IS_DAY|CONDITION_TEXT|CONDITION_ICON|CONDITION_CODE|WIND_KPH|WIND_MPH|WIND_DEGREE|WIND_DIR|PRESSURE_MB|PRESSURE_IN|PRECIP_MM|PRECIP_IN|HUMIDITY|CLOUD|FEELSLIKE_C|FEELSLIKE_F|VIS_KM|VIS_MILES|UV|GUST_KPH|GUST_MPH|RECORD_TIMESTAMP|
  -----------------------------

* Lambda will insert the processed data into Snowflake using Snowflake connectors.

---

### 4️⃣ **Streamlit (Visualization Layer / Web App)**

* A Streamlit app is used to build an interactive dashboard.

* Responsibilities:

  * Query data from Snowflake.
  * Generate visualizations like temperature trends, humidity comparisons across cities, etc.
  * Provide an easy-to-use web interface for users.

* Streamlit can be hosted on a simple server or cloud service to provide browser-based access.

---

## ⚙️ **End-to-End Flow Summary**

1. 🔁 **Fetch:** Lambda periodically triggers and fetches data from the weather API.
2. 🔄 **Transform & Load:** Lambda processes the data and inserts it into the Snowflake database.
3. 📊 **Query & Visualize:** Streamlit queries data from Snowflake and builds dashboards that end users can interact with to see weather insights.

---

## 🚀 **Advantages of this architecture**

✅ **Scalable & Serverless:**

* Using Lambda and Snowflake means minimal infra management and easy scaling.

✅ **Pay-as-you-go:**

* Lambda and Snowflake pricing is based on usage.

✅ **Quick UI:**

* Streamlit makes it fast to prototype and build dashboards.

✅ **Separation of Concerns:**

* Lambda handles ETL, Snowflake handles storage and analytics, Streamlit handles presentation.

''')

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