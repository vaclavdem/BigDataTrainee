USE DATABASE flight_dwh;


CREATE OR REPLACE TABLE stage.stg_flights_raw (

    passenger_id STRING,
    first_name STRING,
    last_name STRING,
    gender STRING,
    age STRING,
    nationality STRING,

    airport_name STRING,
    airport_country_code STRING,
    country_name STRING,
    airport_continent STRING,
    continents STRING,

    departure_date STRING,
    arrival_airport STRING,
    pilot_name STRING,

    flight_status STRING,
    ticket_type STRING,
    passenger_status STRING,

    load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);


CREATE OR REPLACE TABLE transform.clean_flights (

    passenger_id STRING,
    first_name STRING,
    last_name STRING,
    gender STRING,
    age NUMBER,
    nationality STRING,

    airport_name STRING,
    airport_country_code STRING,
    country_name STRING,
    continent STRING,

    departure_date DATE,
    arrival_airport STRING,
    pilot_name STRING,

    flight_status STRING,
    ticket_type STRING,
    passenger_status STRING,

    load_ts TIMESTAMP
);



CREATE OR REPLACE STREAM transform.clean_flights_stream
ON TABLE transform.clean_flights;



CREATE OR REPLACE TABLE mart.dim_passenger (
    passenger_key NUMBER IDENTITY PRIMARY KEY,
    passenger_id STRING,
    first_name STRING,
    last_name STRING,
    gender STRING,
    age NUMBER,
    nationality STRING
);


CREATE OR REPLACE TABLE mart.dim_airport (
    airport_key NUMBER IDENTITY PRIMARY KEY,
    airport_name STRING,
    country_name STRING,
    continent STRING,
    airport_country_code STRING
);


CREATE OR REPLACE TABLE mart.dim_pilot (
    pilot_key NUMBER IDENTITY PRIMARY KEY,
    pilot_name STRING
);


CREATE OR REPLACE TABLE mart.fact_flights (

    flight_key NUMBER IDENTITY PRIMARY KEY,

    passenger_key NUMBER,
    airport_key NUMBER,
    pilot_key NUMBER,

    ticket_type STRING,
    flight_status STRING,
    passenger_status STRING,

    flight_cnt NUMBER DEFAULT 1,

    load_ts TIMESTAMP
);



CREATE OR REPLACE TABLE audit.etl_log (
    process_name STRING,
    rows_affected NUMBER,
    status STRING,
    run_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);



CREATE OR REPLACE SECURE VIEW mart.v_fact_flights AS
SELECT * FROM mart.fact_flights;