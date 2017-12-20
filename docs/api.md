## Eventsum API
Eventsum is ultimately a tool to aggregate arbitrary events. In order to aggregate events, eventsum provides various 
ways for the user to specify 'rules' or 'filters' to apply to the events. This way, eventsum can be used for any type 
of data aggregation.  

For a full working example, please look at [this example](/cmd/example.go).

### `eventsum.New(configFilename string)`
Initializes a new eventsum server using the config file. 

Arguments: 
```
configFilename: string 
```

Returns: `*eventsum.EventsumServer` 

Example: 
```
package main
    
import (
    "github.com/ContextLogic/eventsum"
)
    
func main() {
    e := eventsum.New("config_filename.json")
}
```

## EventsumServer functions
### `EventsumServer.Start()`
Starts the server, listening on the port specified by config file.

Example: 
```
func main() {
    e := eventsum.New("config_filename.json")
    e.Start()
}
```

### `EventsumServer.AddFilter(name, filter)`
Adds a user-specified filter function to the eventsum server. The filter function will be called on `EventData` every
time `name` is in the request. 

Arguments: 
```
name: string 
filter: func (EventData)(EventData, error)
```

Returns: `error` 

Example: 
```
import (
    "github.com/ContextLogic/eventsum"
    "github.com/ContextLogic/eventsum/models"
    "github.com/pkg/errors"
)
    
func main() {
    e := eventsum.New("config_filename.json")
    e.AddFilter("filter1", filter1)
    e.Start()
}
    
func filter1(data models.EventData) (models.EventData, error) {
    if d, ok := data.Raw.(int); !ok {
        return data, errors.New("not an int!")
    } else {
        data.Raw = d + 1
    }
    
    return data, nil
}
```

### `EventsumServer.AddGrouping(name string, grouping func)`
Adds a user-specified grouping function to the eventsum server. This function merges a single `EventData` to a 
summarized group. This is useful if events need to be aggregated in a unique manner, rather than simply counting. For 
example, if histogram information needs to be saved, this would be useful.

 
The grouping function will be called on `EventData` after all the filter functions have been executed. 

Arguments: 
```
name: string 
grouping: func(EventData, map[string]interface{}) (map[string]interface{}, error)
```

Returns: `error` 

Example: 
```
import (
    "github.com/ContextLogic/eventsum"
    "github.com/ContextLogic/eventsum/models"
    "github.com/pkg/errors"
)
    
func main() {
    e := eventsum.New("config_filename.json")
    e.AddGrouping("grouping1", grouping1)
    e.Start()
}
    
func grouping1(data models.EventData, group map[string]interface{}) (map[string]interface{}, error) {
    if _, ok := group["b"]; !ok {
        group["b"] = 0.0
    }
    
    if d, ok := data.Raw.(float64); !ok {
        return group, errors.New("not a float!")
    } else {
        i := group["b"].(float64)
        group["b"] = i + d
    }
    
    return group, nil
}
```

### `EventsumServer.AddConsolidation(f func)`
Adds a user-specified consolidation function to the eventsum server. This function merges two summarized groups into 
one. This is useful if events need to be aggregated in a unique manner, rather than simply counting. For example, if 
histogram information needs to be saved, this would be useful.

 
The grouping function will be called after all the filter and grouping functions have been executed. This merges local
groups into the remote groups in the db.

Arguments: 
```
f: func(map[string]interface{}, map[string]interface{}) (map[string]interface{}, error)
```

Returns: `error` 

Example: 
```
import (
    "github.com/ContextLogic/eventsum"
    "github.com/ContextLogic/eventsum/models"
    "github.com/pkg/errors"
    "math"
)
    
func main() {
    e := eventsum.New("config_filename.json")
    e.AddConsolidation(consolidate1)
    e.Start()
}
    
func consolidate1(group1, group2 map[string]interface{}) (map[string]interface{}, error) {
    for k, i := range group1 {
        if v, ok := group2[k]; !ok {
            group2[k] = v
        } else {
            group2[k] = math.Max(v.(float64), i.(float64))
        }
    }
    return group2, nil
}
```
