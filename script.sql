DROP TABLE IF EXISTS event_instance_period;
DROP TABLE IF EXISTS event_instance;
DROP TABLE IF EXISTS event_base;
DROP TABLE IF EXISTS event_detail;
DROP TABLE IF EXISTS event_group;


CREATE TABLE IF NOT EXISTS event_group (
  _id serial8 PRIMARY KEY,
  name varchar(512) UNIQUE,
  info text
);

INSERT INTO event_group (_id, name, info) VALUES (0, 'default', 'default group');

CREATE TABLE IF NOT EXISTS event_base (
  _id serial8 PRIMARY KEY,
  service_id int8 DEFAULT NULL,
  event_type varchar(32),
  event_name varchar(512),
  event_group_id int8 REFERENCES event_group(_id) DEFAULT 0,
  event_environment_id int8,
  processed_data json,
  processed_data_hash varchar(64),
  UNIQUE (service_id, event_type, event_environment_id, processed_data_hash)
);

CREATE TABLE IF NOT EXISTS event_detail (
  _id serial8 PRIMARY KEY,
  raw_detail json,
  processed_detail json,
  processed_detail_hash varchar(64),
  UNIQUE (processed_detail_hash)
);

CREATE TABLE IF NOT EXISTS event_instance (
  _id serial8 PRIMARY KEY,
  event_base_id int8 REFERENCES event_base(_id),
  event_detail_id int8 REFERENCES event_detail(_id),
  event_environment_id int8,
  raw_data json,
  generic_data json,
  generic_data_hash varchar(64),
  event_message text,
  UNIQUE (generic_data_hash, event_environment_id)
);

CREATE TABLE IF NOT EXISTS event_instance_period (
  _id serial8 PRIMARY KEY,
  event_instance_id int8 REFERENCES event_instance(_id),
  start_time timestamp,
  end_time timestamp,
  updated timestamp,
  count int8 DEFAULT 1,
  counter_json jsonb,
  cas_value int8 DEFAULT 0,
  UNIQUE (event_instance_id, start_time, end_time)
);