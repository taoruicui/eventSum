package main

import (
	"math"
	"regexp"

	"github.com/ContextLogic/eventsum"
	c "github.com/ContextLogic/eventsum/config"
	"github.com/ContextLogic/eventsum/models"
	"github.com/jessevdk/go-flags"
	m "github.com/mitchellh/mapstructure"
	logger "github.com/sirupsen/logrus"
)

func main() {
	var config c.Flags
	parser := flags.NewParser(&config, flags.Default)
	_, err := parser.Parse()
	if err != nil {
		logger.Fatal(err)
	}
	e := eventsum.New(config.ConfigFile)
	e.AddFilter("exception_python_remove_line_no", exceptionPythonRemoveLineNo)
	e.AddFilter("exception_python_process_stack_vars", exceptionPythonProcessStackVars)
	e.AddFilter("exception_python_remove_stack_vars", exceptionPythonRemoveStackVars)
	//e.AddGrouping("query_perf_trace_grouping", queryPerfTraceGrouping)
	//e.AddConsolidation(consolidationFunction)
	e.Start()
}

/*
FILTER FUNCTIONS

In order to implement a configurable filter, the function must accept an EventData
and return (EventData, error)
*/

func exceptionPythonRemoveLineNo(data models.EventData) (models.EventData, error) {
	var stacktrace models.StackTrace
	err := m.Decode(data.Raw, &stacktrace)
	if err != nil {
		return data, err
	}
	for i := range stacktrace.Frames {
		stacktrace.Frames[i].LineNo = 0
	}
	data.Raw = stacktrace
	return data, nil
}

func exceptionPythonRemoveStackVars(data models.EventData) (models.EventData, error) {

	stacktrace := models.StackTrace{}
	err := m.Decode(data.Raw, &stacktrace)
	if err != nil {
		return data, err
	}
	for i := range stacktrace.Frames {
		stacktrace.Frames[i].Vars = nil
	}
	data.Raw = stacktrace
	return data, nil
}

func exceptionPythonProcessStackVars(data models.EventData) (models.EventData, error) {

	stacktrace := models.StackTrace{}
	err := m.Decode(data.Raw, &stacktrace)
	if err != nil {
		return data, err
	}

	re := regexp.MustCompile(`0[xX][0-9a-fA-F]+`)

	for i := range stacktrace.Frames {
		if stacktrace.Frames[i].Vars == nil {
			continue
		}

		// Replace memory addresses with placeholder
		for k, v := range stacktrace.Frames[i].Vars {
			if str, ok := v.(string); ok {
				str = re.ReplaceAllString(str, "x")
				stacktrace.Frames[i].Vars[k] = str
			}
		}
	}
	data.Raw = stacktrace
	return data, nil
}

/*
GROUPING FUNCTIONS

In order to implement a grouping, the function must accept an eventData
and a , and modify it in place.
*/

func queryPerfTraceGrouping(data models.EventData, group map[string]interface{}) (map[string]interface{}, error) {
	if _, ok := group["b"]; !ok {
		group["b"] = 0.0
	}
	i := group["b"].(float64)
	group["b"] = i + 1.0
	return group, nil
}

/*
CONSOLIDATION FUNCTION

This function should define how two groups should be merged.
*/

func consolidationFunction(group1, group2 map[string]interface{}) (map[string]interface{}, error) {
	for k, i := range group1 {
		if v, ok := group2[k]; !ok {
			group2[k] = v
		} else {
			group2[k] = math.Max(v.(float64), i.(float64))
		}
	}
	return group2, nil
}
