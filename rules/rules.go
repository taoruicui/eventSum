package rules

import (
	"github.com/pkg/errors"
	. "github.com/ContextLogic/eventsum/models"
	"reflect"
)

type Rule struct {
	Filter          map[string]interface{} // filtering data
	Grouping        map[string]interface{} // merge one event to a group
	ConsolidateFunc interface{}            // merge two groups together
}

// Process user defined groupings. A grouping is how the user wants to map some data to a
// group, or a key. Currently, only counter_json is supported
func (r *Rule) ProcessGrouping(event UnaddedEvent, group map[string]interface{}) (map[string]interface{}, error) {
	for _, name := range event.ConfigurableGroupings {
		if _, ok := r.Grouping[name]; !ok {
			return group, errors.New("Function name not supported")
		}
		res, err := r.call("group", name, event.Data, group)
		if err != nil {
			return group, err
		}
		// Second reflect.Value is the error
		if err, ok := res[1].Interface().(error); ok {
			return group, err
		}
		// First reflect.Value is the EventData
		d := res[0].Interface().(map[string]interface{})
		group = d
	}
	return group, nil
}

// Process user defined filters. Make sure that if there is an error, do not do that processing.
func (r *Rule) ProcessFilter(event UnaddedEvent, filterName string) (UnaddedEvent, error) {
	if funcNames, ok := event.ConfigurableFilters[filterName]; ok {
		for _, name := range funcNames {
			if _, ok := r.Filter[name]; !ok {
				return event, errors.New("Function name not supported")
			}
			res, err := r.call("filter", name, event.Data.Copy())
			if err != nil {
				return event, err
			}
			// Second reflect.Value is the error
			if err, ok := res[1].Interface().(error); ok {
				return event, err
			}
			// First reflect.Value is the EventData
			d := res[0].Interface().(EventData)
			event.Data = d
		}
	}

	return event, nil
}

func (r *Rule) Consolidate(g1 map[string]interface{}, g2 map[string]interface{}) (map[string]interface{}, error) {
	res, err := r.call("consolidate", "", g1, g2)
	if err != nil {
		return g1, err
	}
	// Second reflect.Value is the error
	if err, ok := res[1].Interface().(error); ok {
		return g1, err
	}
	// First reflect.Value is the EventData
	return res[0].Interface().(map[string]interface{}), nil
}

// Calls the Function by name using Reflection
func (r *Rule) call(typ string, name string, params ...interface{}) ([]reflect.Value, error) {
	var f reflect.Value
	if typ == "group" {
		f = reflect.ValueOf(r.Grouping[name])
	} else if typ == "filter" {
		f = reflect.ValueOf(r.Filter[name])
	} else if typ == "consolidate" {
		f = reflect.ValueOf(r.ConsolidateFunc)
	}
	if len(params) != f.Type().NumIn() {
		err := errors.New("The number of params is not adapted.")
		return nil, err
	}
	in := make([]reflect.Value, len(params))
	for k, param := range params {
		in[k] = reflect.ValueOf(param)
	}
	result := f.Call(in)
	return result, nil
}

func (r *Rule) AddFilter(name string, filter func(EventData) (EventData, error)) error {
	r.Filter[name] = filter
	return nil
}

func (r *Rule) AddGrouping(name string, grouping func(EventData, map[string]interface{}) (map[string]interface{}, error)) error {
	r.Grouping[name] = grouping
	return nil
}

func (r *Rule) AddConsolidateFunc(f func(map[string]interface{}, map[string]interface{}) (map[string]interface{}, error)) error {
	r.ConsolidateFunc = f
	return nil
}

func NewRule() Rule {
	return Rule{
		Filter: map[string]interface{}{},
		Grouping: map[string]interface{}{},
		ConsolidateFunc: defaultConsolidate,
	}
}

// Default consolidation function. This function takes two dicts and merges
// them together additively. Returns a single group
func defaultConsolidate(g1, g2 map[string]interface{}) (map[string]interface{}, error) {
	for k, i := range g1 {
		if _, ok := g2[k]; !ok {
			g2[k] = 0.0
		}
		g2[k] = g2[k].(float64) + i.(float64)
	}
	return g2, nil
}
