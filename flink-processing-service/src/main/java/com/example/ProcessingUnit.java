package com.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ProcessingUnit {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql(
                "CREATE TABLE RideTest (\n" +
                        "  `ride_id` STRING,\n" +
                        "  `rider_id` STRING,\n" +
                        "  `driver_id` STRING,\n" +
                        "  `location_id` STRING,\n" +
                        "  `amount` FLOAT,\n" +
                        "  `ride_status` STRING,\n" +
                        "  `request_time` TIMESTAMP(3) METADATA FROM 'timestamp',\n" +
                        "  `processing_time` as PROCTIME()\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'rides',\n" +
                        "  'properties.bootstrap.servers' = 'kafka:29092',\n" +
                        "  'properties.group.id' = 'test',\n" +
                        "  'scan.startup.mode' = 'latest-offset',\n" +
                        "  'format' = 'json'\n" +
                        ");"
        );

        // Kafka sink
        tableEnv.executeSql(
                "CREATE TABLE RiderOut (\n" +
                        "  `ride_id` STRING,\n" +
                        "  `rider_id` STRING,\n" +
                        "  `driver_id` STRING,\n" +
                        "  `location_id` STRING,\n" +
                        "  `amount` FLOAT,\n" +
                        "  `ride_status` STRING,\n" +
                        "  `request_time` TIMESTAMP(3) METADATA FROM 'timestamp',\n" +
                        "  `processing_time` as PROCTIME()\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'riders_out',\n" +
                        "  'properties.bootstrap.servers' = 'kafka:29092',\n" +
                        "  'format' = 'json'\n" +
                        ");"
        );
        
        // Pushing to Kafka sink as well
        tableEnv.executeSql(
            "INSERT INTO RiderOut\n" +
            "SELECT \n" +
            "ride_id, \n" +
            "rider_id, \n" +
            "driver_id, \n" +
            "location_id, \n" +
            "amount * 10, \n" +
            "ride_status, \n" +
            "request_time \n" +
            "FROM RideTest"
        );

        // Postgres sink as well
        tableEnv.executeSql(
            "CREATE TABLE rides (" +
            "   ride_id STRING PRIMARY KEY NOT ENFORCED, " +
            "   rider_id STRING, " +
            "   driver_id STRING, " +
            "   amount DOUBLE, " +
            "   ride_status STRING " +
            ") WITH (" +
            "   'connector' = 'jdbc', " +
            "   'url' = 'jdbc:postgresql://postgres:5432/rides_db', " +
            "   'table-name' = 'rides', " +
            "   'username' = 'postgresuser', " +
            "   'password' = 'postgrespw', " +
            "   'driver' = 'org.postgresql.Driver'" +
            ")"
        );
        // Pushing to Postgres sink as well
        tableEnv.executeSql(
            "INSERT INTO rides\n" +
            "SELECT \n" +
            "ride_id, \n" +
            "rider_id, \n" +
            "driver_id, \n" +
            "amount, \n" +
            "ride_status \n" +
            "FROM RideTest"
        );
    }
}