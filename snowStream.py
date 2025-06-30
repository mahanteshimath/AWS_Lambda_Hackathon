import json
import os
import logging
import requests
from datetime import datetime
from typing import Dict, Any, List
import boto3 # Import boto3 for AWS services like SES and SNS
import snowflake.connector # Import snowflake connector

# Configure logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# --- Configuration (Best practice: use Environment Variables in Lambda) ---
# Weather API Configuration
WEATHER_API_URL = os.environ.get("WEATHER_API_URL", "http://api.weatherapi.com/v1/current.json" )
WEATHER_API_KEY = os.environ.get("WEATHER_API_KEY", "YOUR_WEATHER_API_KEY") # Replace with your actual key or env var

# List of cities to monitor, comma-separated from environment variable
CITIES_TO_MONITOR_STR = os.environ.get("CITIES_TO_MONITOR", "Bengaluru,Mumbai,Delhi,Chennai") # Default Indian cities
CITIES_TO_MONITOR = [city.strip() for city in CITIES_TO_MONITOR_STR.split(",") if city.strip()]

# SES Email Configuration
SENDER_EMAIL = os.environ.get("SENDER_EMAIL", "your-verified-sender-email@example.com") # !!! IMPORTANT: Replace with your SES verified sender email !!!
RECIPIENT_EMAILS_STR = os.environ.get("RECIPIENT_EMAILS", "recipient1@example.com")
RECIPIENT_EMAILS = [email.strip() for email in RECIPIENT_EMAILS_STR.split(",") if email.strip()]

# Snowflake Configuration
SNOWFLAKE_USER = os.environ.get("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.environ.get("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.environ.get("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.environ.get("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.environ.get("SNOWFLAKE_SCHEMA")
SNOWFLAKE_TABLE = os.environ.get("SNOWFLAKE_TABLE", "weather_data") # Default table name

# Initialize AWS clients
ses_client = boto3.client("ses", region_name=os.environ.get("AWS_REGION", "us-east-1"))

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda handler function for weather monitoring, rain notification,
    and storing weather data in Snowflake for multiple cities.
    This function is designed to be triggered by a scheduled EventBridge rule.
    """
    all_weather_records_for_snowflake = [] # NEW: List to collect all records
    all_messages = []

    # Ensure Snowflake table exists before processing any data
    try:
        ensure_snowflake_table_exists()
        logger.info(f"Snowflake table {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} ensured to exist.")
    except Exception as e:
        logger.error(f"Failed to ensure Snowflake table exists: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"message": f"Failed to initialize Snowflake table: {str(e)}"})
        }

    for city in CITIES_TO_MONITOR:
        logger.info(f"Processing weather data for city: {city}...")
        try:
            weather_data = fetch_weather_data(city)

            if not weather_data:
                logger.warning(f"Could not fetch weather data for {city}. Skipping.")
                all_messages.append(f"Failed to fetch weather data for {city}.")
                continue

            current_condition_text = weather_data["current"]["condition"]["text"]
            current_temp_c = weather_data["current"]["temp_c"]
            logger.info(f"Current weather in {city}: {current_condition_text}, Temp: {current_temp_c}°C")

            # --- Notification Logic ---
            notification_message = f"No rain expected for {city} at the moment."
            if is_raining_soon(weather_data):
                logger.info(f"Rain is expected for {city}! Sending notifications.")
                send_notifications(weather_data) # This function now handles city-specific alerts
                notification_message = f"Rain notifications sent for {city}!"
            
            all_messages.append(notification_message)

            # --- Prepare Data for Batch Snowflake Insert ---
            snowflake_record = prepare_weather_data_for_snowflake(weather_data)
            all_weather_records_for_snowflake.append(snowflake_record) # NEW: Add to list

        except Exception as e:
            logger.error(f"Unhandled error processing {city}: {str(e)}")
            all_messages.append(f"Unhandled error for {city}: {str(e)}.")

    # --- NEW: Perform Batch Snowflake Insert AFTER the loop ---
    total_records_inserted = 0
    if all_weather_records_for_snowflake:
        try:
            total_records_inserted = insert_to_snowflake(all_weather_records_for_snowflake)
            logger.info(f"Successfully inserted {total_records_inserted} weather record(s) into Snowflake in batch.")
        except Exception as e:
            logger.error(f"Error inserting batch data to Snowflake: {str(e)}")
            all_messages.append(f"Failed to insert batch data to Snowflake: {str(e)}.")
    else:
        logger.info("No weather records collected for Snowflake insertion.")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "; ".join(all_messages) + f". Total Snowflake records inserted: {total_records_inserted}.",
            "timestamp": datetime.utcnow().isoformat()
        })
    }

def fetch_weather_data(city: str) -> Dict[str, Any] | None:
    """
    Fetches current weather data from the WeatherAPI.
    """
    params = {
        "q": city,
        "key": WEATHER_API_KEY
    }
    try:
        response = requests.get(WEATHER_API_URL, params=params, timeout=10)
        response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching weather data for {city}: {e}")
        return None

def is_raining_soon(weather_data: Dict[str, Any]) -> bool:
    """
    Checks if the current weather condition indicates rain.
    This is a simplified check based on current conditions. For more accuracy,
    you'd typically use forecast data and more sophisticated logic.
    """
    condition_text = weather_data["current"]["condition"]["text"].lower()
    precip_mm = weather_data["current"]["precip_mm"]

    # Keywords that might indicate rain or precipitation
    rain_keywords = ["rain", "drizzle", "shower", "thunderstorm", "sleet", "snow"]

    # Check condition text for rain keywords
    for keyword in rain_keywords:
        if keyword in condition_text:
            logger.info(f"Rain detected by keyword: \'{keyword}\' in \'{condition_text}\'")
            return True

    # Check for non-zero precipitation (even a small amount)
    if precip_mm > 0.0: # Check if there's any recorded precipitation
        logger.info(f"Rain detected by precipitation (precip_mm > 0): {precip_mm}mm")
        return True

    return False

def send_email_notification(weather_data: Dict[str, Any]):
    """
    Sends an email notification about the rain using AWS SES to multiple recipients.
    """
    location_name = weather_data["location"]["name"]
    current_condition = weather_data["current"]["condition"]["text"]
    temp_c = weather_data["current"]["temp_c"]
    feelslike_c = weather_data["current"]["feelslike_c"]
    humidity = weather_data["current"]["humidity"]
    wind_kph = weather_data["current"]["wind_kph"]
    last_updated = weather_data["current"]["last_updated"]

    subject = f"Rain Alert for {location_name}!"
    body_text = f"""
Hello,

This is an automated rain alert for {location_name}.

Current Weather Conditions (as of {last_updated}):
- Condition: {current_condition}
- Temperature: {temp_c}°C (Feels like: {feelslike_c}°C)
- Humidity: {humidity}%
- Wind: {wind_kph} kph

It looks like it's about to rain or is currently raining. Don't forget your umbrella!

Best regards,
Your Weather Notifier
"""

    if not RECIPIENT_EMAILS:
        logger.warning("No recipient emails configured. Skipping email notification.")
        return

    try:
        response = ses_client.send_email(
            Source=SENDER_EMAIL,
            Destination={
                "ToAddresses": RECIPIENT_EMAILS,
            },
            Message={
                "Subject": {
                    "Data": subject
                },
                "Body": {
                    "Text": {
                        "Data": body_text
                    }
                }
            }
        )
        logger.info(f"Email sent! Message ID: {response["MessageId"]}")
    except Exception as e:
        logger.error(f"Error sending email: {e}")
        # Do not re-raise here, allow other notifications to proceed


def send_notifications(weather_data: Dict[str, Any]):
    """
    Orchestrates sending all types of notifications.
    """
    send_email_notification(weather_data)
 

def prepare_weather_data_for_snowflake(weather_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transforms the fetched weather data into a format suitable for Snowflake insertion.
    Matches the proposed Snowflake table schema.
    """
    location = weather_data.get("location", {})
    current = weather_data.get("current", {})
    condition = current.get("condition", {})

    return {
        "location_name": location.get("name"),
        "location_region": location.get("region"),
        "location_country": location.get("country"),
        "location_lat": location.get("lat"),
        "location_lon": location.get("lon"),
        "localtime_epoch": location.get("localtime_epoch"),
        "localtime_str": location.get("localtime"),
        "last_updated_epoch": current.get("last_updated_epoch"),
        "last_updated_str": current.get("last_updated"),
        "temp_c": current.get("temp_c"),
        "temp_f": current.get("temp_f"),
        "is_day": current.get("is_day") == 1, # Convert 1/0 to True/False
        "condition_text": condition.get("text"),
        "condition_icon": condition.get("icon"),
        "condition_code": condition.get("code"),
        "wind_kph": current.get("wind_kph"),
        "wind_mph": current.get("wind_mph"),
        "wind_degree": current.get("wind_degree"),
        "wind_dir": current.get("wind_dir"), # Corrected: wind_dir is under current
        "pressure_mb": current.get("pressure_mb"),
        "pressure_in": current.get("pressure_in"),
        "precip_mm": current.get("precip_mm"),
        "precip_in": current.get("precip_in"),
        "humidity": current.get("humidity"),
        "cloud": current.get("cloud"),
        "feelslike_c": current.get("feelslike_c"),
        "feelslike_f": current.get("feelslike_f"),
        "vis_km": current.get("vis_km"),
        "vis_miles": current.get("vis_miles"),
        "uv": current.get("uv"),
        "gust_kph": current.get("gust_kph"),
        "gust_mph": current.get("gust_mph"),
        "record_timestamp": datetime.utcnow() # Timestamp when this record is processed by Lambda
    }

def get_snowflake_connection():
    """
    Create and return a Snowflake connection using environment variables.
    """
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        logger.info("Successfully connected to Snowflake")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to Snowflake: {str(e)}")
        raise

def ensure_snowflake_table_exists():
    """
    Checks if the Snowflake table exists and creates it if it doesn't.
    """
    conn = None
    cursor = None
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()

        # SQL to create the table if it doesn't exist
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} (
            location_name VARCHAR,
            location_region VARCHAR,
            location_country VARCHAR,
            location_lat FLOAT,
            location_lon FLOAT,
            localtime_epoch BIGINT,
            localtime_str VARCHAR,
            last_updated_epoch BIGINT,
            last_updated_str VARCHAR,
            temp_c FLOAT,
            temp_f FLOAT,
            is_day BOOLEAN,
            condition_text VARCHAR,
            condition_icon VARCHAR,
            condition_code INTEGER,
            wind_kph FLOAT,
            wind_mph FLOAT,
            wind_degree INTEGER,
            wind_dir VARCHAR,
            pressure_mb FLOAT,
            pressure_in FLOAT,
            precip_mm FLOAT,
            precip_in FLOAT,
            humidity INTEGER,
            cloud INTEGER,
            feelslike_c FLOAT,
            feelslike_f FLOAT,
            vis_km FLOAT,
            vis_miles FLOAT,
            uv FLOAT,
            gust_kph FLOAT,
            gust_mph FLOAT,
            record_timestamp TIMESTAMP_NTZ
        );
        """
        logger.info(f"Executing CREATE TABLE IF NOT EXISTS for {SNOWFLAKE_TABLE}")
        cursor.execute(create_table_sql)
        logger.info(f"Table {SNOWFLAKE_TABLE} creation command executed.")

    except Exception as e:
        logger.error(f"Error ensuring Snowflake table exists: {str(e)}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def insert_to_snowflake(weather_records: List[Dict[str, Any]]) -> int:
    """
    Insert weather data records into Snowflake.
    """
    if not weather_records:
        logger.warning("No weather data to insert into Snowflake.")
        return 0

    conn = None
    cursor = None
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()

        # Construct the INSERT statement dynamically based on the keys in the first record
        # This makes it more robust to changes in the data structure
        columns = ", ".join(weather_records[0].keys())
        placeholders = ", ".join([f"%({col})s" for col in weather_records[0].keys()])
        insert_sql = f"INSERT INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} ({columns}) VALUES ({placeholders})"

        # Execute batch insert
        cursor.executemany(insert_sql, weather_records)

        # Commit transaction
        conn.commit()

        insert_count = len(weather_records)
        logger.info(f"Inserted {insert_count} records into Snowflake table {SNOWFLAKE_TABLE}")

        return insert_count

    except Exception as e:
        logger.error(f"Error inserting data to Snowflake: {str(e)}")
        if conn:
            conn.rollback() # Rollback on error
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

