# eventsum
An event aggregate service 

There are a lot of events that might occur throughout production (eg. exceptions, queries, etc). A lot of these events 
might be extremely similar to other events. We want a system that captures the events, aggregates them, and stores them 
in a datastore. This project: 

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
Create a Postgres cluster. Run the following script in your Postgres cluster: 
```
createdb <DBNAME>
psql <DBNAME> -a -f script.sql
```
This should create 5 tables: `event_base`, `event_instance`, `event_instance_period`, `event_group`, and `event_detail`. 

Eventsum runs on dataman. We need to generate a schema.json and instance.yaml file corresponding to the DB. 
```
# install dependencies
go get “github.com/jessevdk/go-flags”
go get "github.com/jacksontj/dataman/src/client"
go get "github.com/jacksontj/dataman/src/client/direct"
go get "github.com/jacksontj/dataman/src/query"
go get "github.com/jacksontj/dataman/src/storage_node"
go get "github.com/jacksontj/dataman/src/storage_node/metadata"
    
cd $GOPATH/src/github.com/jacksontj/dataman/src/schemaexport
go build 
./schemaexport --databases <DBNAME> > $GOPATH/src/github.com/ContextLogic/eventsum/config/schema.json
```
If this command generates an empty schema, that means dataman could not reach your DB. Modify the 
`src/storage_node/storagenode/config.yaml` file, and run the above command again. 

### Setting up Config Files
Eventsum requires a number of configuration files in order to run. Look in the `config/` directory for all config files 
you need. You can look at the documentation of the files [here](/docs/config_files.md)

### Running the server
Look at `example/example.go` for a working example. To run the service, type this:
```
cd $GOPATH/src/github.com/ContextLogic/eventsum
go run cmd/example.go -c `pwd`/config/config.json
```

## Documentation
Please at the [docs](/docs) for the api and grafana integrations. 
