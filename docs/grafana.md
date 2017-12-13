# Grafana Integration

### Setting up Eventsum as a Datasource
Eventsum can act as a datasource in Grafana. All you need to do are a couple of steps: 
1. Install a simple grafana plugin called ‘simple json datasource’.
2. Launch `eventmaster`
3. Configure new Data Source `(Menu > Data Sources > + Add data source)`
   1. give it a name
   2. change Type to SimpleJson
   3. provide the url, including the /grafana suffix, e.g.: http://localhost:8080/grafana
   4. Configure direct accesss
   5. click Save & Test

If it shows green then you are good to go!

![datasource](/docs/assets/add_eventsum_datasource.png "add datasource")

### Adding Template Variables
Now we want to be able to create template variables to filter out exceptions. Right now, the supported variables are: 
- `service_name`
- `event_base_id`
- `group_name`
- `environment_name`
- `sort`
- `limit` 

These variables can be added to the query statements, which we will show later on. Click on the gear at the top of the 
page and press `Templating`. Press `new` and follow this screenshot:

![service](/docs/assets/add_service_name.png "add service name")

Click `update` and the variable should appear on your dashboard. Do the same for the other variables:

![event](/docs/assets/add_event_base.png "add event base")
![group](/docs/assets/add_group_name.png "add groupname")
![environment](/docs/assets/add_environment.png "add environment")
![sort](/docs/assets/add_sort.png "add sort")

All your template variables should be set now!

### Creating a Graph
Now we can make a graph. Click on `Add Row`, and then select the `graph` option. Once we have a graph, click the title, 
press `Edit`, and then go to the `Metrics` panel:

![graph](/docs/assets/add_graph.png "add graph")

Make sure eventsum is set at the datasource. To add a metric, press the button on the left of the eye, and click 
`toggle edit mode`. A box should appear that allows you to type in a metric target. 

Eventsum is built so that users can customize what the metrics to see (eg. top 5 exceptions, time series of a single 
exception, etc). This is done with the use of template variables! One can put the variables in the metric target and 
the the graphs should update accordingly. The syntax of the target is as follows:
```
<variable name>=$<template_variable>[&<variable name>=$<template_variable>...]
```

For example, to select an event with a specific base_id and service_name:
```
service_name=$service_name&event_base_id=$event_base_id
```

To select top 5 events with a specific service_name, sorted:
```
service_name=$service_name&sort=$sort&limit=5
```

To select events with a specific environment_name and group_name:
```
group_name=$group_name&environment_name=$environment_name
```

The value of the `$<template_variable>` is whatever is currently selected on the dashboard. Note that this works even 
for multi-value variables, where events will be filtered (val1 OR val2 OR etc.) on all values. 

Now you can create customs graphs! 
