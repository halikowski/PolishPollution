use database air_db;
use warehouse transform_wh;
use role sysadmin;
use schema curated;


ALTER SESSION SET TIMEZONE = 'Europe/Warsaw';

-- dynamic table for pollution data
CREATE OR REPLACE DYNAMIC TABLE curated_pollution
TARGET_LAG = 'DOWNSTREAM'
WAREHOUSE = TRANSFORM_WH
AS
    WITH unique_pollution AS (
        SELECT
            id,
            record_ts,
            json_data,
            ingestion_ts,
            source_filename,
            stg_file_md5,
            RANK() OVER(PARTITION BY record_ts ORDER BY ingestion_ts DESC) as rank
        FROM raw.staging_pollution
        QUALIFY rank = 1 -- deduplicating at file level
    )
    SELECT
        record_ts,
        res.value:coord.lat::float as lat,
        res.value:coord.lon::float as lon,
        
        res.value:list[0]:main.aqi::int AS aqi,
        res.value:list[0]:components.co::float as co,
        res.value:list[0]:components.no::float as no,
        res.value:list[0]:components.no2::float as no2,
        res.value:list[0]:components.o3::float as o3,
        res.value:list[0]:components.so2::float as so2,
        res.value:list[0]:components.pm2_5::float as pm2_5,
        res.value:list[0]:components.pm10::float as pm10,
        res.value:list[0]:components.nh3::float as nh3,
        
        ingestion_ts,
        source_filename,
        stg_file_md5
        
    FROM unique_pollution,
    LATERAL FLATTEN (input => json_data:results) res

    
SELECT * FROM curated_pollution

-- dynamic table for weather data
CREATE OR REPLACE DYNAMIC TABLE curated_weather
TARGET_LAG = 'DOWNSTREAM'
WAREHOUSE = TRANSFORM_WH
AS
    WITH unique_weather AS (
        SELECT
            id,
            record_ts,
            json_data,
            ingestion_ts,
            source_filename,
            stg_file_md5,
            RANK() OVER(PARTITION BY record_ts ORDER BY ingestion_ts DESC) as rank
        FROM raw.staging_weather
        QUALIFY rank = 1 -- deduplicating at file level
    )
    SELECT
        record_ts,
        res.value:id::int as location_id,
        res.value:name::text as city,
        
        res.value:coord.lat::float as lat,
        res.value:coord.lon::float as lon,
        
        res.value:weather[0]:description::text as weater_con,
        res.value:main.temp::float as temp,
        res.value:main.feels_like::float as feels_like,
        res.value:main.temp_min::float as min_temp,
        res.value:main.temp_max::float as max_temp,
        res.value:main.pressure::float as pressure,
        res.value:main.humidity::float as humidity,
        res.value:visibility::int as visibility,
        res.value:wind.speed::float as wind_speed,
        res.value:wind.deg::int as wind_deg,
        res.value:clouds.all::int as clouds,
    
        ingestion_ts,
        source_filename,
        stg_file_md5,
    
    FROM unique_weather,
    LATERAL FLATTEN (input => json_data:results) res;


SELECT * FROM curated_weather

-- table for population data in polish cities over 100k population
CREATE OR REPLACE TABLE cities_pop (
    city STRING,
    city_ascii STRING,
    lat FLOAT,
    lon FLOAT,
    country_code STRING,
    population INT,
    timezone STRING,
    country STRING
)

-- copy file from github repo to plain internal stage in order for the load_cities_data procedure to work
-- (this sadly does not work inside stored procedure)
COPY FILES
        INTO @air_db.raw.internal_stage/population/
        FROM @air_db.raw.polish_pollution_repo/branches/main/PLcities_over_100k.csv

        
-- Procedure for copying the population data from internal stage file to cities_pop table
CREATE OR REPLACE PROCEDURE load_cities_data()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    -- creating a temporary staging table for upsert operation on cities_pop table
    CREATE OR REPLACE TEMP TABLE staging_cities (
        city STRING,
        city_ascii STRING,
        lat FLOAT,
        lon FLOAT,
        country_code STRING,
        population INT,
        timezone STRING,
        country STRING
    );
    
    -- loading data to the staging table
    COPY INTO staging_cities
    FROM @air_db.raw.internal_stage/population/PLcities_over_100k.csv
    FILE_FORMAT = raw.my_csv_format
    ON_ERROR = 'CONTINUE';

    -- upsert operation on the target table (cities_pop)
    MERGE INTO cities_pop AS target
    USING staging_cities AS source
    ON target.city = source.city AND target.lat = source.lat AND target.lon = source.lon
    WHEN MATCHED THEN
        UPDATE SET 
            city_ascii = source.city_ascii,
            country_code = source.country_code,
            population = source.population,
            timezone = source.timezone,
            country = source.country
    WHEN NOT MATCHED THEN
        INSERT (city, city_ascii, lat, lon, country_code, population, timezone, country)
        VALUES (source.city, source.city_ascii, source.lat, source.lon, source.country_code, source.population, source.timezone, source.country);

    RETURN 'Cities population data load completed successfully';
END;
$$;


call load_cities_data();


select * from cities_pop;
