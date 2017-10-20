package main

import (
	"github.com/pkg/errors"
	"reflect"
	"github.com/mitchellh/mapstructure"
	"log"
)

type StackTrace struct {
	Module   string  `json:"module",mapstructure:"module"`
	Type     string  `json:"type",mapstructure:"type"`
	Value    string  `json:"value",mapstructure:"value"`
	RawStack string  `json:"raw_stack",mapstructure:"raw_stack"`
	Frames   []Frame `json:"frames",mapstructure:"frames"`
}

type Frame struct {
	AbsPath     string                 `json:"abs_path",mapstructure:"abs_path"`
	ContextLine string                 `json:"context_line",mapstructure:"context_line"`
	Filename    string                 `json:"filename",mapstructure:"filename"`
	Function    string                 `json:"function",mapstructure:"function"`
	LineNo      int                    `json:"lineno",mapstructure:"lineno"`
	Module      string                 `json:"module",mapstructure:"module"`
	PostContext []string               `json:"post_context",mapstructure:"post_context"`
	PreContext  []string               `json:"pre_context",mapstructure:"pre_context"`
	Vars        map[string]interface{} `json:"vars",mapstructure:"vars"`
}

type Rule struct {
	Filter map[string]interface{}
	Grouping map[string]interface{}
	log *log.Logger
}

// Process user defined groupings. A grouping is how the user wants to map some data to a
// group, or a key. Currently, only counter_json is supported
func (r *Rule) ProcessGrouping(event UnaddedEvent, eip *EventInstancePeriod) error {
	for _, name := range event.ConfigurableGroupings {
		if _, ok := r.Grouping[name]; !ok {
			r.log.Print("Function name not supported")
			continue
		}
		_, err := r.call("group", name, event, eip)
		if err != nil {
			r.log.Printf("Error: %v", err)
			continue
		}
	}
	return nil
}

// Process user defined filters. Make sure that if there is an error, do not do that processing.
func (r *Rule) ProcessFilter(event UnaddedEvent, filterName string) (interface{}, error) {
	if funcNames, ok := event.ConfigurableFilters[filterName]; ok {
		for _, name := range funcNames {
			if _, ok := r.Filter[name]; !ok {
				r.log.Print("Function name not supported")
				continue
			}
			res, err := r.call("filter", name, event)
			if err != nil {
				r.log.Printf("Error: %v", err)
				continue
			}
			// Second reflect.Value is the error
			if err, ok := res[1].Interface().(error); ok {
				r.log.Printf("Error: %v", err)
				continue
			}
			// First reflect.Value is the UnaddedEvent
			e := res[0].Interface().(UnaddedEvent)
			event = e
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

// Calls the Function by name using Reflection
func (r Rule) call(typ string, name string, params ...interface{}) (result []reflect.Value, err error) {
	var f reflect.Value
	if typ == "group" {
		f = reflect.ValueOf(r.Grouping[name])
	} else if typ == "filter" {
		f = reflect.ValueOf(r.Filter[name])
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

func newRule(l *log.Logger) Rule {
	return Rule{
		Filter: map[string]interface{}{
			"exception_python_remove_line_no":    exceptionPythonRemoveLineNo,
			"exception_python_remove_stack_vars": exceptionPythonRemoveStackVars,
		},
		Grouping: map[string]interface{} {
			"query_perf_trace_grouping": queryPerfTraceGrouping,
		},
		log: l,
	}
}

/*
FILTER FUNCTIONS

In order to implement a configurable filter, the function must accept an UnaddedEvent
and return (UnaddedEvent, error)
*/

func exceptionPythonRemoveLineNo(event UnaddedEvent) (UnaddedEvent, error) {
	var stacktrace StackTrace
	err := mapstructure.Decode(event.Data.Raw, &stacktrace)
	if err != nil {
		return event, errors.New("Cannot type assert to ExceptionData")
	}
	for i := range stacktrace.Frames {
		stacktrace.Frames[i].LineNo = 0
	}
	event.Data.Raw = stacktrace
	return event, nil
}

func exceptionPythonRemoveStackVars(event UnaddedEvent) (UnaddedEvent, error) {
	var stacktrace StackTrace
	err := mapstructure.Decode(event.Data.Raw, &stacktrace)
	if err != nil {
		return event, errors.New("Cannot type assert to ExceptionData")
	}
	for i := range stacktrace.Frames {
		stacktrace.Frames[i].Vars = nil
	}
	event.Data.Raw = stacktrace
	return event, nil
}

/*
GROUPING FUNCTIONS

In order to implement a grouping, the function must accept an UnaddedEvent
and a *EventInstancePeriod, and modify it in place.
 */

func queryPerfTraceGrouping(ue UnaddedEvent, evt *EventInstancePeriod) {

}