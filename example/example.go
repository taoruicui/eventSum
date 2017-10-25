package example

import (
	"github.com/ContextLogic/eventsum"
	"github.com/mitchellh/mapstructure"
)

type stackTrace struct {
	Module   string  `json:"module",mapstructure:"module"`
	Type     string  `json:"type",mapstructure:"type"`
	Value    string  `json:"value",mapstructure:"value"`
	RawStack string  `json:"raw_stack",mapstructure:"raw_stack"`
	Frames   []frame `json:"frames",mapstructure:"frames"`
}

type frame struct {
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

func main() {
	e := eventsum.New("/home/jwen/ContextLogic/eventsum/config/default.json")
	e.AddFilter("exception_python_remove_line_no", exceptionPythonRemoveLineNo)
	e.AddFilter("exception_python_remove_stack_vars", exceptionPythonRemoveStackVars)
	e.AddGrouping("query_perf_trace_grouping", queryPerfTraceGrouping)
	e.Start()
}

func exceptionPythonRemoveLineNo(data eventsum.EventData) (eventsum.EventData, error) {
	var stacktrace stackTrace
	err := mapstructure.Decode(data.Raw, &stacktrace)
	if err != nil {
		return data, err
	}
	for i := range stacktrace.Frames {
		stacktrace.Frames[i].LineNo = 0
	}
	data.Raw = stacktrace
	return data, nil
}

func exceptionPythonRemoveStackVars(data eventsum.EventData) (eventsum.EventData, error) {
	var stacktrace stackTrace
	err := mapstructure.Decode(data.Raw, &stacktrace)
	if err != nil {
		return data, err
	}
	for i := range stacktrace.Frames {
		stacktrace.Frames[i].Vars = nil
	}
	data.Raw = stacktrace
	return data, nil
}

func queryPerfTraceGrouping(data eventsum.EventData, group map[string]interface{}) map[string]interface{} {
	if _, ok := group["b"]; !ok {
		group["b"] = 0.0
	}
	i := group["b"].(float64)
	group["b"] = i + 1.0
	return group
}