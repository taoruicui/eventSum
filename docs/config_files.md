This document is to explain the various config files that eventsum requires. 

## config.json
This is the main configuration file for eventsum. This will call all other config files in the directory. 

### `data_source_instance`
Absolute path to the `datasourceinstance.yaml` file. 

### `data_source_schema`
Absolute path to the `schema.json` file. 

### `log_config_file`
Absolute path to the `logconfig.json` file. 

### `database_name`
Database name. This will set prom metrics to log under the database_name. Note that this will not affect the 
schema.json. Therefore, if the database_name is changed, the schema.json will have to be regenerated. Default name is 
`eventsum`.  

### `event_batch_limit`
Batch size limit for events. Int. This will be the maximum number of events in the queue before the events are 
processed. Default is 5.  

### `event_time_limit`
Time limit in seconds before events are processed in the queue. Int. Default is 5.

### `time_format`
String format of how time will be represented. Default is `"2006-01-02 15:04:05"`. All requests to eventsum that require 
time will follow this time format (eg. /capture, /search, etc.)

### `server_port`
Local port that the server will listen on. Default is `8080`.

### `services`
Map of all the services that are accepted. The key will be the name, and the value will be the service_id. 

Example: 
```
{
    "default": {"service_id": 0},
    "wish_fe": {"service_id": 1},
    "wish_be": {"service_id": 2},
    "merchant_fe": {"service_id": 3},
    "merchant_be": {"service_id": 4}
}
```

### `environments`
Map of all the environments that are accepted. The key will be the env name, and the value will be the env id. 

Example:
```
{
    "default": {"environment_id": 0},
    "prod": {"environment_id": 1},
    "dev": {"environment_id": 2},
    "stage": {"environment_id": 3},
    "sandbox": {"environment_id": 4}
}
```

### `time_interval`
This specifies the time interval in minutes that eventsum will group on for `event_instance_period`. Default is `10`. 
The larger the value, more events will be grouped together, but will lose granularity. The smaller the value, the less 
events will be grouped, and will retain granularity. 


## logconfig.json
This is the file to handle logging

### `app_logging`
Directory that eventsum will save its application logs to. 

### `data_logging`
Directory that eventsum will save the event data to. 

### `log_save_data_interval`
Time interval in minutes to save the data logs

### `log_data_period_check_interval`
time interval in minutes to move the data logs to the database

### `environment`
“dev” or “prod”. If dev, logging will be directed to STDOUT and STDERR. If prod, it will be directed to the directories 
specified above

## schema.json
This is the json schema file of your database that [dataman](https://github.com/jacksontj/dataman) requires. 

## dataourceinstance.yaml
This is the database connection file that [dataman](https://github.com/jacksontj/dataman) requires. 