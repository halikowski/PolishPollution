use database air_db;
use schema consumption;
use role sysadmin;
use warehouse transform_wh;

CREATE OR REPLACE FUNCTION prominent_index(no float, no2 float, o3 float, so2 float, pm2_5 float, pm10 float, nh3 float)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
HANDLER = 'prominent_index'
AS
$$
def prominent_index(no, no2, o3, so2, pm2_5, pm10, nh3):
    # Mapping column names
    # This function does not take CO into consideration, as it's norm  in ug/m3 is approx. 10x higher than other pollutants
    values = {
        'NO': no,
        'NO2': no2,
        'O3': o3,
        'SO2': so2,
        'PM2_5': pm2_5,
        'PM10': pm10,
        'NH3': nh3
        }
    # Returning the highest value index
    max_value = max(values, key=values.get)
    return max_value
$$;

-- dynamic table for merging data from curated_pollution, curated_weather and cities_pop tables with some additional columns
CREATE OR REPLACE DYNAMIC TABLE final_conditions_wide
TARGET_LAG = '30 min'
WAREHOUSE = 'TRANSFORM_WH'
AS
SELECT
    cp.record_ts,
    year(cp.record_ts) as meas_year,
    quarter(cp.record_ts) as meas_quarter,
    month(cp.record_ts) as meas_month,
    day(cp.record_ts) as meas_day,
    hour(cp.record_ts) as meas_hour,
    pop.city as city,
    pop.population as population,
    cp.lat as lat,
    cp.lon as lon,
    aqi as aqi,
    co as co,
    no as no,
    no2 as no2,
    o3 as o3,
    so2 as so2,
    pm2_5 as pm2_5,
    pm10 as pm10,
    nh3 as nh3,
    prominent_index(no, no2, o3, so2, pm2_5, pm10, nh3) as prominent_pollutant,
    weater_con as weather_con,
    temp,
    feels_like,
    min_temp,
    max_temp,
    pressure,
    humidity,
    visibility,
    wind_speed,
    wind_deg,
    clouds

FROM curated.curated_pollution cp
INNER JOIN curated.curated_weather cw
ON cp.record_ts = cw.record_ts AND cp.lat = cw.lat AND cp.lon = cw.lon
LEFT JOIN curated.cities_pop as pop
-- coordinates approximation is required for joining population data since the API response tends to round random values
ON LEFT(cp.lat,5) = LEFT(pop.lat,5) AND LEFT(cp.lon,5) = LEFT(pop.lon,5);

select * from final_conditions_wide;

-- DIMENSION & FACT TABLES
CREATE OR REPLACE DYNAMIC TABLE date_dim
TARGET_LAG = 'DOWNSTREAM'
WAREHOUSE = 'TRANSFORM_WH'
AS
WITH date_cte AS(
    SELECT
        record_ts as measurement_time,
        meas_year,
        meas_quarter,
        meas_month,
        meas_day,
        meas_hour,
    FROM final_conditions_wide
    GROUP BY 1,2,3,4,5,6
)
SELECT 
    hash(measurement_time) as date_pk,
    *
FROM date_cte
ORDER BY measurement_time DESC;

CREATE OR REPLACE DYNAMIC TABLE location_dim
TARGET_LAG = 'DOWNSTREAM'
WAREHOUSE = 'TRANSFORM_WH'
AS
WITH geo_cte AS(
    SELECT
        city,
        population,
        lat,
        lon
    FROM final_conditions_wide
    GROUP BY 1,2,3,4
)
SELECT
    hash(lat,lon) as location_pk,
    *
FROM geo_cte
ORDER BY city;

CREATE OR REPLACE DYNAMIC TABLE conditions_fact
TARGET_LAG = '30 min'
WAREHOUSE = 'TRANSFORM_WH'
AS
SELECT
    hash(record_ts,lat,lon) as measurement_pk,
    hash(record_ts) as date_fk,
    hash(lat,lon) as location_fk,
    aqi,
    prominent_pollutant,
    weather_con,
    co,
    no,
    no2,
    o3,
    so2,
    pm2_5,
    pm10,
    nh3,
    temp,
    feels_like,
    min_temp,
    max_temp,
    pressure,
    humidity,
    visibility,
    wind_speed,
    wind_deg,
    clouds
FROM final_conditions_wide;


-- test query
select measurement_pk, measurement_time, city, aqi, prominent_pollutant, weather_con
from conditions_fact cf
INNER JOIN location_dim loc
ON cf.location_fk = loc.location_pk
INNER JOIN date_dim d
ON cf.date_fk = d.date_pk;
    
