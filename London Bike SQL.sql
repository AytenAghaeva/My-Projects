--Importing Dataset

CREATE TABLE london(
	timestampp TIMESTAMP, 
	cnt REAL, 
	temp1 REAL, 
	temp2 REAL,
	hum REAL, 
	wind_speed REAL, 
	weather_code REAL, 
	is_holiday REAL, 
	is_weekend REAL, 
	season REAL
	);

COPY london 
FROM 'C:\London Bike Project\london_merged.csv' 
DELIMITER ',' CSV HEADER;

SELECT * FROM london;

--Dividing the timestamp column into 2 columns: DATE and TIME columns

ALTER TABLE london
ADD COLUMN event_date DATE,
ADD COLUMN event_time TIME;

ALTER TABLE london 
RENAME COLUMN "timestampp" 
TO event_timestamp;

SELECT 
    event_timestamp, 
    DATE(event_timestamp) AS event_date,
    TIMETZ(event_timestamp) AS event_time
FROM 
    london; 


UPDATE london
SET
    event_date = DATE(event_timestamp),
    event_time = TIMETZ(event_timestamp);

ALTER TABLE london
DROP COLUMN event_timestamp;


--Data Cleaning: 1) Checking for nulls; 2) Detecting outliers
SELECT 
    COUNT(*) AS total_rows,
    COUNT(*) FILTER (WHERE temp1 IS NULL) AS temp1_nulls,
    COUNT(*) FILTER (WHERE temp2 IS NULL) AS temp2_nulls,
    COUNT(*) FILTER (WHERE wind_speed IS NULL) AS wind_speed_nulls,
    COUNT(*) FILTER (WHERE hum IS NULL) AS humidity_nulls,
    COUNT(*) FILTER (WHERE weather_code IS NULL) AS weather_code_nulls,
    COUNT(*) FILTER (WHERE is_holiday IS NULL) AS is_holiday_nulls,
    COUNT(*) FILTER (WHERE is_weekend IS NULL) AS is_weekend_nulls,
    COUNT(*) FILTER (WHERE season IS NULL) AS season_nulls,
	COUNT(*) FILTER (WHERE event_date IS NULL) AS date_nulls
FROM london;

--IQR(interquartile) method

WITH quartiles AS (
    SELECT 
        percentile_cont(0.25) WITHIN GROUP (ORDER BY cnt) AS Q1,
        percentile_cont(0.75) WITHIN GROUP (ORDER BY cnt) AS Q3
    FROM london
),
iqr_values AS (
    SELECT (Q3 - Q1) AS IQR FROM quartiles
)
SELECT cnt, Q1, Q3, IQR
FROM london, quartiles, iqr_values
WHERE cnt < (Q1 - 1.5 * IQR) OR cnt > (Q3 + 1.5 * IQR);

--with Z_score

WITH stats AS (
    SELECT 
        ROUND(AVG(cnt)) AS mean,
        ROUND(STDDEV(cnt)) AS stddev
    FROM london
)
SELECT 
    cnt, 
    stats.mean, 
    stats.stddev,
    ROUND((cnt - stats.mean) / stats.stddev) AS z_score
FROM 
    london, stats
WHERE 
    ABS((cnt - stats.mean) / stats.stddev) > 3;

--Descriptive Statistic Analysis using methods such as count, mean, minimum, maximum
SELECT 
    COUNT(*) AS total_records,
    ROUND(AVG(cnt))AS avg_count,
    MIN(cnt) AS min_count,
    MAX(cnt) AS max_count,
    ROUND(AVG(temp1)) AS avg_temp1,
    MIN(temp1) AS min_temp1,
    MAX(temp1) AS max_temp1,
    ROUND(AVG(temp2)) AS avg_temp2,
    MIN(temp2) AS min_temp2,
    MAX(temp2) AS max_temp2,
    ROUND(AVG(wind_speed)) AS avg_wind_speed,
    MIN(wind_speed) AS min_wind_speed,
    MAX(wind_speed) AS max_wind_speed,
    ROUND(AVG(hum)) AS avg_humidity,
    MIN(hum) AS min_humidity,
    MAX(hum) AS max_humidity
FROM london;


--CHECKING COUNT OF UNIQE VALUES IN CATEGORICAL COLUMNS

SELECT 
    COUNT(DISTINCT weather_code) AS unique_weather_codes,
    COUNT(DISTINCT is_holiday) AS unique_holidays,
    COUNT(DISTINCT is_weekend) AS unique_weekends,
    COUNT(DISTINCT season) AS unique_seasons
FROM london;

--BIKE SHARE STATISTICS DEPENDING ON WEATHER

SELECT 
    weather_code,
    round(AVG(cnt)) AS avg_bike_count,
    round(SUM(cnt)) AS total_bike_count,
	MAX(cnt) AS max_bike_count,
	MIN(cnt) AS min_bike_count,
    COUNT(*) AS records_count
FROM london
GROUP BY weather_code
ORDER BY weather_code;


--SEASONAL BIKE SHARE COUNTS

SELECT 
    season,
    COUNT(cnt) AS rentals
FROM london
GROUP BY season
ORDER BY season;

--WEEKEND/WEEKDAY RENTALS

SELECT 
    is_weekend,
    COUNT(cnt) AS rentals
FROM london
GROUP BY is_weekend
ORDER BY is_weekend;

--HOLIDAY RENTALS

SELECT 
	is_holiday,
    COUNT(cnt) AS holiday_rentals
FROM london
WHERE is_holiday = 1
GROUP BY is_holiday;


--DAILY RENTALS

SELECT 
    event_date AS rental_date,
    SUM(cnt) AS total_rentals
FROM london
GROUP BY rental_date
ORDER BY total_rentals DESC
LIMIT 3;
 
--CORRELATION BETWEEN ACTUAL TEMPERATURE, WIND SPEED, HUMIDITY AND BIKE RENTALS

SELECT 
    CORR(temp1, cnt) AS corr_temp_rentals,
	CORR(wind_speed, cnt) AS corr_wind_speed,
	CORR(hum, cnt) AS corr_humidity
FROM london;

--MONTHLY BIKE COUNTS

SELECT 
    EXTRACT(MONTH FROM event_date) AS month,
    COUNT(cnt) AS bike_count
FROM london
GROUP BY month
ORDER BY month;

--HOURLY BIKE COUNTS

SELECT 
    EXTRACT(HOUR FROM event_time) AS rental_hour,
    ROUND(AVG(cnt)) AS avg_rentals
FROM london
GROUP BY rental_hour
ORDER BY rental_hour;

--YEARLY GROWTH RATE OF BIKES

WITH yearly_totals AS (
    SELECT 
        EXTRACT(YEAR FROM event_date) AS year,
        COUNT(cnt) AS bike_count
    FROM london
    GROUP BY year
)
SELECT 
    year,
    bike_count,
    LAG(bike_count) OVER (ORDER BY year) AS previous_year_rentals,
    (bike_count - LAG(bike_count) OVER (ORDER BY year)) * 100.0 /
	LAG(bike_count) OVER (ORDER BY year) AS growth_rate
FROM yearly_totals;


--SEASONAL RENTAL PERCENTAGE

WITH bike_count AS (
    SELECT COUNT(cnt) AS count
    FROM london
)
SELECT 
    season,
    COUNT(cnt) AS season_rentals,
    (COUNT(cnt) * 100.0 / (SELECT count FROM bike_count)) AS percentage_of_count
FROM london
GROUP BY season
ORDER BY season;

--PREDICTING FUTURE VALUES DEPENDING ON FEELS LIKE TEMPERATURE BY USING LINEAR REGRESSION 

WITH temp_rentals AS (
    SELECT 
        temp2,
        COUNT(cnt) AS bike_count
    FROM london
    GROUP BY temp2
)
SELECT 
    temp2,
    bike_count,
    ROUND(REGR_SLOPE(bike_count, temp2) OVER ()) AS slope,
    ROUND(REGR_INTERCEPT(bike_count, temp2) OVER ()) AS intercept
FROM temp_rentals;

--RENTALS BY TEMPERATURE RANGE

SELECT 
    CASE 
        WHEN temp2 < 0 THEN 'Freezing'
        WHEN temp2 >= 0 AND temp2 < 10 THEN 'Cold'
        WHEN temp2 >= 10 AND temp2 < 20 THEN 'Mild'
        WHEN temp2 >= 20 THEN 'Warm'
        ELSE 'Other'  
    END AS temperature_category,
    COUNT(cnt) AS bike_count
FROM london
GROUP BY temperature_category;

--Daily Rental Count Variability

SELECT 
    event_date,
    SUM(cnt) AS total_rentals,
    ROUND(STDDEV(cnt)) AS rental_variability
FROM london
GROUP BY event_date
ORDER BY event_date;


--RENTALS PER SEASON (WITH SEASON NAMES)
WITH seasonal_rentals AS (
    SELECT 
        CASE 
            WHEN EXTRACT(MONTH FROM event_date) IN (12, 1, 2) THEN 'Winter'
            WHEN EXTRACT(MONTH FROM event_date) IN (3, 4, 5) THEN 'Spring'
            WHEN EXTRACT(MONTH FROM event_date) IN (6, 7, 8) THEN 'Summer'
            WHEN EXTRACT(MONTH FROM event_date) IN (9, 10, 11) THEN 'Fall'
        END AS season,
        cnt
    FROM london
)
SELECT 
    season,
    COUNT(cnt) AS bike_count
FROM seasonal_rentals
GROUP BY season
ORDER BY season;


--USER ANALYSIS


WITH user_segments AS (
    SELECT 
        cnt,
        CASE 
            WHEN cnt > 1000 THEN 'Frequent Users'
            WHEN cnt BETWEEN 20 AND 1000 THEN 'Occasional Users'
            ELSE 'Rare Users'
        END AS user_segment
    FROM london
)
SELECT 
    user_segment,
    COUNT(*) AS user_count
FROM user_segments
GROUP BY user_segment
ORDER BY user_segment;


--SEASONAL WEATHER AFFECT ON RENTAL

SELECT 
    season,
    ROUND(AVG(cnt)) AS avg_rentals,
    ROUND(AVG(temp1)) AS avg_temp
FROM london
GROUP BY season
ORDER BY season;
