package eventsum

import (
	"github.com/pkg/errors"
	"log"
	"reflect"
)

type rule struct {
	Filter          map[string]interface{} // filtering data
	Grouping        map[string]interface{} // merge one event to a group
	ConsolidateFunc interface{}            // merge two groups together
	log             *log.Logger
}

// Process user defined groupings. A grouping is how the user wants to map some data to a
// group, or a key. Currently, only counter_json is supported
func (r *rule) ProcessGrouping(event unaddedEvent, group map[string]interface{}) (map[string]interface{}, error) {
	for _, name := range event.ConfigurableGroupings {
		if _, ok := r.Grouping[name]; !ok {
			r.log.Print("Function name not supported")
			continue
		}
		res, err := r.call("group", name, event.Data, group)
		if err != nil {
			r.log.Printf("Error: %v", err)
			continue
		}
		d := res[0].Interface().(map[string]interface{})
		group = d
	}
	return group, nil
}

// Process user defined filters. Make sure that if there is an error, do not do that processing.
func (r *rule) ProcessFilter(event unaddedEvent, filterName string) (interface{}, error) {
	if funcNames, ok := event.ConfigurableFilters[filterName]; ok {
		for _, name := range funcNames {
			if _, ok := r.Filter[name]; !ok {
				r.log.Print("Function name not supported")
				continue
			}
			res, err := r.call("filter", name, event.Data)
			if err != nil {
				r.log.Printf("Error: %v", err)
				continue
			}
			// Second reflect.Value is the error
			if err, ok := res[1].Interface().(error); ok {
				r.log.Printf("Error: %v", err)
				continue
			}
			// First reflect.Value is the EventData
			d := res[0].Interface().(EventData)
			event.Data = d
		}
	}

	if filterName == "data" {
		return event.Data, nil
	} else if filterName == "detail" {
		return event.ExtraArgs, nil
	} else {
		return event, errors.New("filter name not supported")
	}
}

func (r *rule) Consolidate(g1 map[string]interface{}, g2 map[string]interface{}) (map[string]interface{}, error) {
	res, err := r.call("consolidate", "", g1, g2)
	if err != nil {
		r.log.Printf("Error: %v", err)
		return g1, err
	}
	return res[0].Interface().(map[string]interface{}), nil
}

// Calls the Function by name using Reflection
func (r *rule) call(typ string, name string, params ...interface{}) (result []reflect.Value, err error) {
	var f reflect.Value
	if typ == "group" {
		f = reflect.ValueOf(r.Grouping[name])
	} else if typ == "filter" {
		f = reflect.ValueOf(r.Filter[name])
	} else if typ == "consolidate" {
		f = reflect.ValueOf(r.ConsolidateFunc)
	}
	if len(params) != f.Type().NumIn() {
		err = errors.New("The number of params is not adapted.")
		return
	}
	in := make([]reflect.Value, len(params))
	for k, param := range params {
		in[k] = reflect.ValueOf(param)
	}
	result = f.Call(in)
	return
}

func (r *rule) addFilter(name string, filter func(EventData) (EventData, error)) error {
	r.Filter[name] = filter
	return nil
}

func (r *rule) addGrouping(name string, grouping func(EventData, map[string]interface{}) map[string]interface{}) error {
	r.Grouping[name] = grouping
	return nil
}

func (r *rule) addConsolidateFunc(f func(map[string]interface{}, map[string]interface{}) map[string]interface{}) error {
	r.ConsolidateFunc = f
	return nil
}

func newRule(l *log.Logger) rule {
	return rule{
		Filter: map[string]interface{}{
		//"exception_python_remove_line_no":    exceptionPythonRemoveLineNo,
		//"exception_python_remove_stack_vars": exceptionPythonRemoveStackVars,
		},
		Grouping: map[string]interface{}{
		//"query_perf_trace_grouping": queryPerfTraceGrouping,
		},
		ConsolidateFunc: defaultConsolidate,
		log:             l,
	}
}

// Default consolidation function. This function takes two dicts and merges
// them together additively. Returns a single group
func defaultConsolidate(g1, g2 map[string]interface{}) map[string]interface{} {
	for k, i := range g1 {
		if _, ok := g2[k]; !ok {
			g2[k] = 0.0
		}
		g2[k] = g2[k].(float64) + i.(float64)
	}
	return g2
}
