DROP TABLE IF EXISTS event_instance_period;
DROP TABLE IF EXISTS event_instance;
DROP TABLE IF EXISTS event_base;
DROP TABLE IF EXISTS event_detail;


CREATE TABLE IF NOT EXISTS event_base (
  _id serial8 PRIMARY KEY,
  service_id int8,
  event_type varchar(32),
  event_name varchar(512),
  processed_data json,--varchar(64000),
  processed_data_hash varchar(64),
  UNIQUE (service_id, event_type, processed_data_hash)
);

CREATE TABLE IF NOT EXISTS event_detail (
  _id serial8 PRIMARY KEY,
  raw_detail json,--varchar(64000),
  processed_detail json,--varchar(64000),
  processed_detail_hash varchar(64),
  UNIQUE (processed_detail_hash)
);

CREATE TABLE IF NOT EXISTS event_instance (
  _id serial8 PRIMARY KEY,
  event_base_id int8 REFERENCES event_base(_id),
  event_detail_id int8 REFERENCES event_detail(_id),
  raw_data json,--varchar(64000),
  raw_data_hash varchar(64),
  UNIQUE (raw_data_hash)
);

CREATE TABLE IF NOT EXISTS event_instance_period (
  _id serial8 PRIMARY KEY,
  event_instance_id int8 REFERENCES event_instance(_id),
--   event_data_id int8 REFERENCES event_data(_id),
  start_time timestamp with TIME ZONE,
  updated timestamp with TIME ZONE,
  time_interval SMALLINT,
  count int8,
  counter_json json,
  UNIQUE (event_instance_id, start_time, time_interval)
);

