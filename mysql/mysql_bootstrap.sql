CREATE DATABASE IF NOT EXISTS cabservice;
USE cabservice;

GRANT ALL PRIVILEGES ON cabservice.* TO 'mysqluser';
GRANT FILE on *.* to 'mysqluser';

CREATE USER 'debezium' IDENTIFIED WITH mysql_native_password BY 'dbz';

GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium';

FLUSH PRIVILEGES;


CREATE TABLE IF NOT EXISTS cabservice.rides
(
    id SERIAL PRIMARY KEY,
    ride_id VARCHAR(255),
    rider_id VARCHAR(255),
    driver_id VARCHAR(255),
    location_id VARCHAR(255),
    amount FLOAT,
    ride_status VARCHAR(100),
    start_lat DECIMAL(10, 8),
    start_lng DECIMAL(10, 8),
    dest_lat DECIMAL(10, 8),
    dest_lng DECIMAL(10, 8),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
   
LOAD DATA INFILE '/var/lib/mysql-files/data/rides.csv' 
INTO TABLE cabservice.rides 
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
IGNORE 1 LINES
(ride_id,rider_id,driver_id,location_id,amount,ride_status,start_lat,start_lng,dest_lat,dest_lng);
