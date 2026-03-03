DROP TABLE mart.dim_pilot;
UNDROP TABLE mart.dim_pilot;


"""
restore to version that was 5 minutes earlier
"""
CREATE OR REPLACE TABLE mart.dim_passenger
AS
SELECT *
FROM mart.dim_passenger
AT (OFFSET => -300);

"""
восстановление удалённых данных
"""
INSERT INTO mart.fact_flights
SELECT *
FROM mart.fact_flights
BEFORE (STATEMENT => LAST_QUERY_ID());


"""
просмотр старой версии данных
"""
SELECT *
FROM mart.fact_flights
BEFORE (STATEMENT => LAST_QUERY_ID());