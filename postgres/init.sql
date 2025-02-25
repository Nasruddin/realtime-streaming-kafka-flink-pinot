DO $$ 
BEGIN 
    IF NOT EXISTS (SELECT FROM pg_database WHERE datname = 'rides_db') THEN 
        CREATE DATABASE rides_db;
    END IF;
END $$;

\c rides_db;

CREATE TABLE rides (
    ride_id VARCHAR PRIMARY KEY,
    rider_id VARCHAR,
    driver_id VARCHAR,
    location_id INT,
    amount DOUBLE PRECISION,
    ride_status VARCHAR
);