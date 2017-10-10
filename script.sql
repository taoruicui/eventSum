DROP TABLE IF EXISTS exception_instance_period;
DROP TABLE IF EXISTS exception_instance;
DROP TABLE IF EXISTS exception;
DROP TABLE IF EXISTS exception_data;


CREATE TABLE IF NOT EXISTS event_base (
  _id serial8 PRIMARY KEY,
  service_id int8,
  service_version varchar(32),
  name varchar(512),
  processed_stack varchar(64000),
  processed_stack_hash varchar(64),
  UNIQUE (processed_stack_hash)
);

CREATE TABLE IF NOT EXISTS event_data (
  _id serial8 PRIMARY KEY,
  raw_data varchar(64000),
  processed_data varchar(64000),
  processed_data_hash varchar(64),
  UNIQUE (processed_data_hash)
);

CREATE TABLE IF NOT EXISTS event_instance (
  _id serial8 PRIMARY KEY,
  event_base_id int8 REFERENCES event_base(_id),
  event_data_id int8 REFERENCES event_data(_id),
  raw_stack varchar(64000),
  raw_stack_hash varchar(64),
  UNIQUE (raw_stack_hash)
);

CREATE TABLE IF NOT EXISTS event_instance_period (
  _id serial8 PRIMARY KEY,
  event_instance_id int8 REFERENCES event_instance(_id),
  event_data_id int8 REFERENCES event_data(_id),
  start_time timestamp with TIME ZONE,
  updated timestamp with TIME ZONE,
  time_interval SMALLINT,
  count int8,
  UNIQUE (event_instance_id, event_data_id, start_time, time_interval)
);

