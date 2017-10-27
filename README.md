# eventsum
An event aggregate service 

There are a lot of events that might occur throughout production (eg. exceptions, queries, etc). A lot of these events might be extremely similar to other events. We want a system that captures the events, aggregates them, and stores them in a datastore. This project: 

- Creates an eventsum store
- Provides an API to capture / view events 
- Provides a method of configuring custom filters to apply to events

Link to doc: https://drive.google.com/drive/folders/0B8ifUxQoV1nuSklaaXpZOVFIQWM?usp=sharing


## Setup 
### Dependencies
- Golang 
- Postgres 9.5

### Installation
Download the repo: 
```
git clone https://github.com/ContextLogic/eventsum.git
mv eventsum $GOPATH/src/github.com/ContextLogic/
```

Dependencies are currently fetched using [dep](https://github.com/golang/dep).

Run the following to fetch the dependencies: 
```
cd $GOPATH/src/github.com/ContextLogic/eventsum
dep ensure
```
### Setting up Postgres DB
Run the following script in your Postgres cluster: 
```
createdb <DBNAME>
psql <DBNAME> -a -f script.sql
```
This should create 4 tables: `event_base`, `event_instance`, `event_instance_period`, and `event_detail`. 

Eventsum runs on dataman. We need to generate a schema.json and instance.yaml file corresponding to the DB. 

### Running the server
See `example/example.go` for a working example. 

A config file is required run the service. Look in the `config/` directory for an example. 
