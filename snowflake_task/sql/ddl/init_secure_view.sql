USE DATABASE flight_dwh;


CREATE OR REPLACE TABLE audit.access_rules (
    role_name STRING,
    allowed_ticket_type STRING
);


INSERT INTO audit.access_rules (role_name, allowed_ticket_type) VALUES
('ACCOUNTADMIN', 'FIRST_CLASS'),
('ACCOUNTADMIN', 'BUSINESS'),
('ACCOUNTADMIN', 'ECONOMY'),
('JUNIOR_ANALYST', 'ECONOMY');


CREATE OR REPLACE ROW ACCESS POLICY mart.ticket_access_policy
AS (ticket_value STRING) RETURNS BOOLEAN ->
    EXISTS (
        SELECT 1
        FROM audit.access_rules
        WHERE role_name = CURRENT_ROLE()
          AND allowed_ticket_type = ticket_value
    );


CREATE OR REPLACE SECURE VIEW mart.v_fact_flights
WITH ROW ACCESS POLICY mart.ticket_access_policy ON (ticket_type)
AS
SELECT
    flight_key,
    passenger_key,
    airport_key,
    pilot_key,
    ticket_type,
    flight_status,
    passenger_status,
    flight_cnt,
    load_ts
FROM mart.fact_flights;


INSERT INTO mart.fact_flights (ticket_type, flight_status) VALUES
('FIRST_CLASS', 'On Time'),
('ECONOMY', 'Delayed');