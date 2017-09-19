package worker


var WISH_FE_QUERY = `
SELECT _timestamp,
	exception,
	traceback,
	_uri,
	_monitor_key,
	_response_status,
	_remote_ip,
	_likely_bot,
	_suspicious_ip,
	_no_monitor_logging
FROM exceptions WHERE time > {0} AND time < {1}
`

var MERCH_FE_QUERY = `
SELECT time,
	exception,
	traceback,
	_uri,
	_monitor_key,
	_response_status,
	_remote_ip,
	_no_monitor_logging
FROM merchant_exceptions WHERE time > {0} AND time < {1}
AND exception is not null
`

var BE_QUERY = `
SELECT time,
	traceback,
	queue,
	message,
	no_monitor_logging
FROM exceptions_be WHERE time > {0} AND time < {1}
`

var WISHPOST_FE_QUERY = `
SELECT time,
	exception,
	traceback,
	_uri,
	_monitor_key,
	_response_status,
	_remote_ip,
	_no_monitor_logging
FROM merchant_exceptions WHERE time > {0} AND time < {1}
AND exception is not null
`

var WISHPOST_BE_QUERY = `
SELECT time,
	traceback,
	queue,
	message,
	no_monitor_logging
FROM exceptions_be WHERE time > {0} AND time < {1}
`

var LEMMINGS_QUERY = `
SELECT time,
	exception,
	traceback,
	_uri,
	_remote_ip,
	_no_monitor_logging
FROM lemmings_exceptions WHERE time > {0} AND time < {1}
`

var QueryMap = map[string]string{
	"WISH_FE": WISH_FE_QUERY,
	"MERCH_FE": MERCH_FE_QUERY,
	"WISH_AND_MERCH_BE": BE_QUERY,
	"WISHPOST_FE": WISHPOST_FE_QUERY,
	"WISHPOST_BE": WISHPOST_BE_QUERY,
	"LEMMINGS": LEMMINGS_QUERY,
}

