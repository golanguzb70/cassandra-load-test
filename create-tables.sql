CREATE  KEYSPACE IF NOT EXISTS cassandra_test WITH REPLICATION = { 
   'class' : 'SimpleStrategy', 
   'replication_factor' : 1 
} AND DURABLE_WRITES = true;

USE cassandra_test;


CREATE TABLE IF NOT EXISTS cassandra_test.driver (
    id UUID PRIMARY KEY,
    lat double,
    lng double,
    geo_hash varchar, 
    score int,
    phone_charge_percent int,
    active boolean,
    last_updated_time int 
);