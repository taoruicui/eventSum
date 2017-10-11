package main

import (
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"reflect"
)

type Filter map[string]interface{}

func (f *Filter) Process(event UnaddedEvent, filterName string) (string, error) {
	if funcNames, ok := event.ConfigurableFilters[filterName]; ok {
		for _, name := range funcNames {
			res, err := f.call(name, event)
			if err != nil {
				fmt.Printf("Error: %v", err)
				continue
			}
			// Second Value is the error
			if err, ok := res[1].Interface().(error); ok {
				fmt.Printf("Error: %v", err)
				continue
			}
			// First Value is the UnaddedEvent
			e := res[0].Interface().(UnaddedEvent)
			event = e
		}
	}

	if filterName == "data" {
		res, err := json.Marshal(event.Data)
		return string(res), err
	} else if filterName == "detail" {
		res, err := json.Marshal(event.ExtraArgs)
		return string(res), err
	} else {
		return "", errors.New("filter name not supported")
	}
}

// Calls the Function by name using Reflection
func (filter Filter) call(name string, params ...interface{}) (result []reflect.Value, err error) {
	f := reflect.ValueOf(filter[name])
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

func newFilter() Filter {
	return Filter{
		"exception_python_remove_line_no":    exceptionPythonRemoveLineNo,
		"exception_python_remove_stack_vars": exceptionPythonRemoveStackVars,
	}
}

/*
FILTER FUNCTIONS

In order to implement a configurable filter, the function must accept an UnaddedEvent
and return (UnaddedEvent, error)
*/

func exceptionPythonRemoveLineNo(event UnaddedEvent) (UnaddedEvent, error) {
	return event, nil
}

func exceptionPythonRemoveStackVars(event UnaddedEvent) (UnaddedEvent, error) {
	return event, nil
}
