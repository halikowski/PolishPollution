use role accountadmin;

-- git integration for my PolishPollution repository. This is a function 'still in preview'
-- trying it out mainly to get population file from the repo
create or replace api integration my_git_integration
    api_provider = git_https_api
    api_allowed_prefixes = ('https://github.com/halikowski/')
    enabled = true;

-- connecting repository
create or replace git repository polish_pollution_repo
    api_integration = my_git_integration
    origin = 'https://github.com/halikowski/PolishPollution';

    
show git branches in git repository polish_pollution_repo;

ALTER SESSION SET TIMEZONE = 'Europe/Warsaw';

-- warehouse creation (separate for 3 steps)
create warehouse load_wh
    with 
    warehouse_size = 'small' 
    warehouse_type = 'standard' 
    auto_suspend = 60 
    auto_resume = true;

create warehouse transform_wh
    with 
    warehouse_size = 'small' 
    warehouse_type = 'standard' 
    auto_suspend = 60 
    auto_resume = true;

create warehouse bi_wh
    with 
    warehouse_size = 'small' 
    warehouse_type = 'standard' 
    auto_suspend = 60 
    auto_resume = true
    comment = 'this warehouse is created only for BI solutions incl. Streamlit';


grant usage on warehouse load_wh to role sysadmin;
grant usage on warehouse transform_wh to role sysadmin;
grant usage on warehouse bi_wh to role sysadmin;

use role sysadmin;
use warehouse load_wh;

-- database creation
create database if not exists air_db;
use database air_db;

create schema if not exists raw;
create schema if not exists curated;
create schema if not exists consumption;

use schema raw;

use role accountadmin;
-- AWS S3 storage integration
create or replace storage integration my_s3_int
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'S3'
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::654654564428:role/snowflake-si-role'
STORAGE_ALLOWED_LOCATIONS = ('s3://mateuszairqualitydata/')

DESC INTEGRATION my_s3_int;


use role sysadmin;

CREATE OR REPLACE FILE FORMAT my_json_format
    type = json
    strip_outer_array = true
    compression = auto
    comment = 'File format for .json files';

CREATE OR REPLACE FILE FORMAT my_csv_format
    type = 'csv'
    compression = auto
    skip_header=1
    FIELD_OPTIONALLY_ENCLOSED_BY = '\042'
    field_delimiter = ';'
    record_delimiter = '\n'
    comment = 'File format for .csv files';

-- additional internal stage for .csv files like the file with population data
CREATE OR REPLACE STAGE internal_stage
FILE_FORMAT = my_csv_format;

CREATE OR REPLACE STAGE pollution_s3_stage
URL = 's3://mateuszairqualitydata/raw/pollution'
STORAGE_INTEGRATION = my_s3_int
FILE_FORMAT = my_json_format
COMMENT = 'S3 stage for the pollution data directory';

CREATE OR REPLACE STAGE weather_s3_stage
URL = 's3://mateuszairqualitydata/raw/weather'
STORAGE_INTEGRATION = my_s3_int
FILE_FORMAT = my_json_format
COMMENT = 'S3 stage for the weather data directory';

-- EXTERNAL STAGES (AWS S3)

CREATE OR REPLACE TRANSIENT TABLE staging_pollution (
    id INT PRIMARY KEY autoincrement,
    record_ts TIMESTAMP NOT NULL,
    json_data VARIANT,
    ingestion_ts TIMESTAMP_NTZ,
    source_filename STRING,
    stg_file_md5 STRING
);

CREATE OR REPLACE TRANSIENT TABLE staging_weather (
    id INT PRIMARY KEY autoincrement,
    record_ts TIMESTAMP NOT NULL,
    json_data VARIANT,
    ingestion_ts TIMESTAMP_NTZ,
    source_filename STRING,
    stg_file_md5 STRING
);

GRANT ALL PRIVILEGES ON stage weather_s3_stage to SYSADMIN;
GRANT ALL PRIVILEGES ON stage pollution_s3_stage to SYSADMIN;
GRANT ALL PRIVILEGES ON stage internal_stage to SYSADMIN;
GRANT ALL PRIVILEGES ON file format my_json_format to SYSADMIN;
GRANT ALL PRIVILEGES ON file format my_csv_format to SYSADMIN;

-- SNOWPIPES

CREATE OR REPLACE PIPE pollution_pipe
    AUTO_INGEST = TRUE
    AS COPY INTO staging_pollution (record_ts, json_data, ingestion_ts, source_filename, stg_file_md5)
    FROM (
        SELECT
            TO_TIMESTAMP($1:results[0]:list[0]:dt),
            $1,
            CURRENT_TIMESTAMP(),
            METADATA$FILENAME,
            METADATA$FILE_CONTENT_KEY
            
        FROM
            @pollution_s3_stage
    );
    
CREATE OR REPLACE PIPE weather_pipe
    AUTO_INGEST = TRUE
    AS COPY INTO staging_weather (record_ts, json_data, ingestion_ts, source_filename, stg_file_md5)
    FROM (
        SELECT
            TO_TIMESTAMP($1:results[0]:dt),
            $1,
            CURRENT_TIMESTAMP(),
            METADATA$FILENAME,
            METADATA$FILE_CONTENT_KEY
            
        FROM
            @weather_s3_stage
    );

-- REFRESHING PIPES
ALTER PIPE pollution_pipe REFRESH;
ALTER PIPE weather_pipe REFRESH;

-- TABLE CHECKUP
SELECT * FROM staging_pollution;
SELECT * from staging_weather;
list @pollution_s3_stage;
