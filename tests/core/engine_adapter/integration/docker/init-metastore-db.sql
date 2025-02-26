-- Settings
alter system set max_connections to '500';

-- Spark
create database metastore;

-- Trino
create database datalake_metastore;
create database datalake_iceberg_metastore;
create database datalake_delta_metastore;
create database testing_metastore;
create database testing_iceberg_metastore;
create database testing_delta_metastore;
create database nessie;