# Import python packages
import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

# Write directly to the app
st.title("Air pollution in Poland")
st.write(
    """This app shows current and historical air pollution in Poland, accompanied by
weather info. You can choose from 48 Polish cities (or big Warsaw districts) with population over 100k.
All indexes are shown in µg/m3.
    """
)

# Get the current credentials
session = get_active_session()

# Variables for holding the selection parameters (city and date)
city_opt, date_opt = '',''

# Query to obtain list of distinct cities available
city_query = """
    SELECT city FROM AIR_DB.CONSUMPTION.CONDITIONS_FACT_DAY_AGG
    GROUP BY city
    ORDER BY 1 DESC;
"""
# Run the query and create list of city names
city_list = session.sql(city_query).collect()
city_names = [row['CITY'] for row in city_list]

# Create dropdown list for cities
city_opt = st.selectbox('Select City', city_names)

# If city chosen, create date dropdown list
if (city_opt is not None and len(city_opt) > 1):
    date_query = f"""
        SELECT measurement_date
        FROM AIR_DB.CONSUMPTION.CONDITIONS_FACT_DAY_AGG
        WHERE city = '{city_opt}'
        GROUP BY measurement_date
        ORDER BY 1 DESC;
    """

    date_list = session.sql(date_query).collect()
    date_names = [row['MEASUREMENT_DATE'] for row in date_list]
    date_opt = st.selectbox('Select Date', date_names)

# If date chosen, create checkbox for switching between all-in-1 or specific pollutant
if (date_opt is not None and not st.checkbox('Specific pollutant')):
    trend_sql = f"""
        SELECT
            hour(measurement_time) as Hour,
            co_avg,
            no_avg,
            no2_avg,
            o3_avg,
            so2_avg,
            pm2_5_avg,
            pm10_avg,
            nh3_avg
        FROM AIR_DB.CONSUMPTION.CONDITIONS_FACT_DAY_AGG
        WHERE city = '{city_opt}'
            AND measurement_date = '{date_opt}'
        ORDER BY measurement_date
    """
    sf_df = session.sql(trend_sql).collect()

    pd_df = pd.DataFrame(
        sf_df,
        columns = ['Hour','CO','NO','NO2','O3','SO2','PM2_5','PM10','NH3']
    )
    
# If checkbox clicked, provide pollutant dropdown list
elif (date_opt is not None):
    pollutant_opt = ''
    pollutants_list = ['CO', 'NO', 'NO2', 'O3', 'SO2', 'PM2_5', 'PM10', 'NH3']
    pollutant_opt = st.selectbox('Pollutant', pollutants_list)

    trend_sql = f"""
        SELECT
            hour(measurement_time) as Hour,
            {pollutant_opt}_avg
        FROM AIR_DB.CONSUMPTION.CONDITIONS_FACT_DAY_AGG
        WHERE city = '{city_opt}'
            AND measurement_date = '{date_opt}'
        ORDER BY measurement_date
    """


    sf_df = session.sql(trend_sql).collect()
    
    pd_df = pd.DataFrame(
        sf_df,
        columns = ['Hour',f'{pollutant_opt}']
    )

# Bar chart creation for pollutants 
st.bar_chart(pd_df, x='Hour')
st.divider()

# Query to obtain basic weather data for chosen city at chosen date
aqi_weather_sql = f"""
    SELECT
        hour(measurement_time) as Hour,
        aqi_avg,
        temp_avg,
        wind_speed_avg
    FROM AIR_DB.CONSUMPTION.CONDITIONS_FACT_DAY_AGG
    WHERE city = '{city_opt}'
            AND measurement_date = '{date_opt}'
        ORDER BY measurement_date
"""

aqi_weather_sf_df = session.sql(aqi_weather_sql).collect()
aqi_weather_df = pd.DataFrame(aqi_weather_sf_df, columns=['Hour','AQI','Temp','WindSpeed'])
# Create line chart showing AQI, temperature and wind speed
st.line_chart(aqi_weather_df, x='Hour')


# Get coordinates for all locations
map_sql = f"""
    SELECT
        l.lat,
        l.lon,
        f.aqi
        FROM AIR_DB.CONSUMPTION.CONDITIONS_FACT f
        INNER JOIN
        AIR_DB.CONSUMPTION.LOCATION_DIM l
        ON f.location_fk = l.location_pk
        WHERE
            date_fk = (
            SELECT date_pk FROM AIR_DB.CONSUMPTION.DATE_DIM
            ORDER BY date_pk DESC
            LIMIT 1
            )
    """
# Get coordinates for single, chosen location
single_map_sql = f"""
    SELECT
        l.lat,
        l.lon
    FROM AIR_DB.CONSUMPTION.CONDITIONS_FACT f
    INNER JOIN
    AIR_DB.CONSUMPTION.LOCATION_DIM l
    ON f.location_fk = l.location_pk
    WHERE l.city = '{city_opt}'
"""
sf_single_map_df = session.sql(single_map_sql).collect()
sf_full_map_df = session.sql(map_sql).collect()

full_map_df = pd.DataFrame(sf_full_map_df, columns=['lat','lon','AQI'])
single_map_df = pd.DataFrame(sf_single_map_df, columns=['lat','lon'])

# Create map charts for all and for single location
st.map(single_map_df)
st.map(full_map_df, size='AQI')
