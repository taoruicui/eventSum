DROP TABLE IF EXISTS exception_instance_period;
DROP TABLE IF EXISTS exception_instance;
DROP TABLE IF EXISTS exception;
DROP TABLE IF EXISTS exception_data;


CREATE TABLE IF NOT EXISTS exception (
  _id serial8 PRIMARY KEY,
  service_id int8,
  service_version varchar(32),
  name varchar(512),
  processed_stack varchar(64000),
  processed_stack_hash varchar(64),
  UNIQUE (processed_stack_hash)
);

CREATE TABLE IF NOT EXISTS exception_data (
  _id serial8 PRIMARY KEY,
  raw_data varchar(64000),
  processed_data varchar(64000),
  processed_data_hash varchar(64),
  UNIQUE (processed_data_hash)
);

CREATE TABLE IF NOT EXISTS exception_instance (
  _id serial8 PRIMARY KEY,
  exception_id int8 REFERENCES exception(_id),
  exception_data_id int8 REFERENCES exception_data(_id),
  raw_stack varchar(64000),
  raw_stack_hash varchar(64),
  UNIQUE (raw_stack_hash)
);

CREATE TABLE IF NOT EXISTS exception_instance_period (
  _id serial8 PRIMARY KEY,
  exception_instance_id int8 REFERENCES exception_instance(_id),
  exception_data_id int8 REFERENCES exception_data(_id),
  created_at timestamp,
  updated_at timestamp,
  count int8,
  UNIQUE (exception_instance_id, exception_data_id)
);

