# AWS_Lambda_Hackathon

## Overview
This project provides a scalable, serverless weather insights dashboard for major Indian cities, leveraging AWS Lambda, Snowflake, and Streamlit. It fetches real-time weather data, stores it in a Snowflake data warehouse, and visualizes it interactively for end users.

## Architecture
![alt text](image.png)
- **Weather API**: External weather data is fetched periodically (e.g., hourly) from a third-party API.
- **AWS Lambda (ETL)**: A Lambda function (named `snowstream` in region `us-west-2`) is triggered to fetch, clean, and load weather data into Snowflake. The Streamlit app can trigger this Lambda function on demand.
- **Snowflake**: Stores all weather data in a structured table for historical and analytical queries.
- **Streamlit App**: Provides a web dashboard for querying and visualizing weather data by city/location, including temperature, humidity, wind speed, and trends over time.

## Features
- **Trigger Lambda from UI**: The dashboard button triggers the AWS Lambda function to fetch and push the latest data to Snowflake.
- **Secure Credentials**: All Snowflake and AWS credentials are managed via Streamlit secrets and session state.
- **Location-based Filtering**: Users can filter and visualize weather data by `LOCATION_NAME`.
- **Rich Visualizations**: Interactive charts for temperature, humidity, wind speed, and time trends using Plotly.
- **Recommendations**: AI-driven suggestions for improving air quality in Indian cities.

## File Structure
- `Home.py`: Main entry point, sets up navigation and app config.
- `pages/Architecture.py`: Shows the architecture diagram and explains the data flow.
- `pages/Realtime_Weather_Across_India.py`: Main dashboard for weather data, Lambda trigger, and visualizations.
- `src/`: Contains images and architecture diagram assets.

## Developers
- **Darshan**
- **Mahantesh**

## How to Run
1. Install requirements: `pip install -r requirements.txt`
2. Set up your `secrets.toml` with Snowflake and AWS credentials.
3. Run the app: `streamlit run Home.py`

---
Developed with ❤️ by Darshan and Mahantesh.
