# Eventsum Endpoints
## Backend Endpoints
For the backend component, there is only one endpoint, to listen for incoming events.

### Capture
```
POST /capture
Content-Type: application/json
```

Captures an incoming event.

Request Format:
```
{
    "service": <string> tier name (eg. WISH_FE, WISH_BE, etc),
    “environment”: <string> environment name(eg. prod, stage, etc),
    "event_name": <string> user-friendly name of event (eg. exception),
    "event_type": <string> subcategory of service_id (eg Go, Python etc),
    "event_data": <reference to data object>,
    "extra_args": <object> args (locals, globals, etc),
    "timestamp": <string> time in UTC. Time format is set in config, "2006-01-02 15:04:05" by default,
    “configurable_filters”: {
        “instance”: [string array], // filters to transform raw -> instance
        “base”: [string array], // filters to transform instance -> base
        “extra_args”: [string array] // filters applied to extra_args
    },
    “configurable_groupings”: [string array]
}

```

Example Request: 

```
{
    "extra_args": {},
    "event_type": "python",
    "service": "WISH FE",
    "event_name": "ZeroDivisionError",
    "environment": "PROD",
    "timestamp": "2017-12-13 21:23:10",
    "configurable_filters": {
        "instance": ["exception_python_process_stack_vars"],
        "base": ["exception_python_remove_line_no", "exception_python_remove_stack_vars"],
        "extra_args": []
    },
    "event_data": {
        "raw_data": {
            "frames": [{
                "function": "<module>",
                "abs_path": "/home/jwen/ContextLogic/test/server.py",
                "pre_context": ["def func4(a):", "    return func3(a)", "", "if __name__ == '__main__':", "    try:"],
                "post_context": ["    except Exception as e:", "        exc_typ, exc_value, tb = sys.exc_info()", "        data = {}", "        data['timestamp'] = datetime.utcnow().strftime(\"%Y-%m-%d %H:%M:%S\")", "        data['service'] = 'WISH FE'"],
                "module": "__main__",
                "filename": "test/server.py",
                "lineno": 21,
                "context_line": "        print func4(3)"
            }, {
                "function": "func4",
                "abs_path": "/home/jwen/ContextLogic/test/server.py",
                "pre_context": ["", "def func3(a):", "    return func2(a)", "", "def func4(a):"],
                "post_context": ["", "if __name__ == '__main__':", "    try:", "        print func4(3)", "    except Exception as e:"],
                "module": "__main__",
                "filename": "test/server.py",
                "lineno": 17,
                "context_line": "    return func3(a)"
            }]
        },
        "message": "integer division or modulo by zero"
    },
    "configurable_groupings": []
}
```

Response: `200` or `400` or `500` status code

### Assign Group
```
POST /assign_group
Content-Type: application/json
```

Assigns the event group for an event base. Can take in an array.

Request format: 
```
{
    "event_id": <id of event base>
    "group_id": <id of event group>
}
``` 

Example Request: 
```
[
    {"event_id": 1, "group_id": 3},
    {"event_id": 2, "group_id": 2},
    {"event_id": 3, "group_id": 1}
]
```

Response: `200` or `400 ` or `500` status code

## Frontend Endpoint
For the frontend component, there will be a dashboard (similar to sentry and gator) that includes different ways of 
viewing the events. The actual dashboard will be built using opsdb, while the go service will serve the content. 

### Search

```
GET /search
```

Returns events that match the search query params.

Required Params:
```
{
    "service_id": <Wish FE, BE, etc.>
}
```

Optional Params:
```
{
    "start_time": <start time in UTC. Time format is set in config, "2006-01-02 15:04:05" by default.>
    "end_time": <end time in UTC. Time format is set in config, "2006-01-02 15:04:05" by default.>
    “hash”: <hash of the event base>
    “group_id”: <id of group to match by>
    “env_id”: <id of environment to match by>
    "limit": <limit the results>
    “sort”: <recent, increased>
    “keywords”: <word to match by>
}
```

Example: 
```
http://localhost:8080/search?service_id=1&start_time=2006-01-02 15:04:05&end_time=2020-01-02 15:04:05
```

Returns:
```
{
    "events": [{
        "id": event base id,
        "event_type": exception type,
        "event_name": event message,
        "event_group_id": group id,
        "event_environment_id": environment id,
        "total_count": occurrences of event,
        "processed_data": event data,
        "instance_ids”: instances matching base event,
        "datapoints”: [{“count”: count, “start”: unix time in ms}],
    }]
}
```

### Details
```
GET /detail
```

Returns the specific details of an event

Required Params:
```
{
    "event_instance_id": <id of specific event>
}
```

Example: 
```
http://localhost:8080/detail?event_id=234
```

Returns:
```
{
    "service_id": service id,
    "event_type": event type,
    "event_name": event message,
    "raw_data": <object> event data,
    "raw_details": <object> extra details (args)
}
```

### Assign Group
```
POST /assign_group
Content-Type: application/json
```

Assigns the group id for multiple event bases. 

Required Params:
```
[{
    "event_id": <id of specific event base>,
    "group_id": <id of event group>
}]
```

Example: 
```
[
    {
        "event_id": 12,
        "groupd_id": 2
    }, {
        "event_id": 13,
        "groupd_id": 2
    }
]
```

Returns: `200` or `400` or `500` status code