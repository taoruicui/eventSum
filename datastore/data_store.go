package datastore

import (
	"context"
	"encoding/json"
	"github.com/ContextLogic/eventsum/util"
	"github.com/ContextLogic/eventsum/rules"
	"github.com/ContextLogic/eventsum/metrics"
	. "github.com/ContextLogic/eventsum/models"
	"github.com/jacksontj/dataman/src/client"
	"github.com/jacksontj/dataman/src/client/direct"
	"github.com/jacksontj/dataman/src/query"
	"github.com/jacksontj/dataman/src/storage_node"
	"github.com/jacksontj/dataman/src/storage_node/metadata"
	"github.com/pkg/errors"
	"io/ioutil"
)

var GlobalRule *rules.Rule

type DataStore interface {
	Query(typ query.QueryType,
		collection string,
		filter interface{},
		record map[string]interface{},
		recordOp map[string]interface{},
		pkey map[string]interface{},
		limit int,
		sort []string,
		join []interface{}) (*query.Result, error)
	AddEvent(evt *EventBase) error
	AddEvents(evts []EventBase) map[int]error
	AddEventInstance(evt *EventInstance) error
	AddEventInstances(evts []EventInstance) map[int]error
	AddEventInstancePeriod(evt *EventInstancePeriod) error
	AddEventinstancePeriods(evts []EventInstancePeriod) map[int]error
	AddEventDetail(evt *EventDetail) error
	AddEventDetails(evts []EventDetail) map[int]error
}

type postgresStore struct {
	client       *datamanclient.Client
}

// Create a new dataStore
func NewDataStore(dataSourceInstance, dataSourceSchema string) (DataStore, error) {
	// Create a connection to Postgres Database through Dataman

	storagenodeConfig, err := storagenode.DatasourceInstanceConfigFromFile(dataSourceInstance)
	if err != nil {
		return nil, err
	}

	// Load meta
	meta := &metadata.Meta{}
	metaBytes, err := ioutil.ReadFile(dataSourceSchema)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal([]byte(metaBytes), meta)
	if err != nil {
		return nil, err
	}

	// TODO: remove
	storagenodeConfig.SkipProvisionTrim = true

	transport, err := datamandirect.NewStaticDatasourceInstanceTransport(storagenodeConfig, meta)
	if err != nil {
		metrics.DBError("transport")
		return nil, err
	}

	client := &datamanclient.Client{Transport: transport}
	return &postgresStore{
		client:       client,
	}, nil
}

func (p *postgresStore) Query(typ query.QueryType,
	collection string,
	filter interface{},
	record map[string]interface{},
	recordOp map[string]interface{},
	pkey map[string]interface{},
	limit int,
	sort []string,
	join []interface{}) (*query.Result, error) {
	var res *query.Result
	q := &query.Query{
		Type: typ,
		Args: map[string]interface{}{
			"db":             "eventsum",
			"collection":     collection,
			"shard_instance": "public",
		},
	}
	if filter != nil {
		q.Args["filter"] = filter
	}
	if record != nil {
		q.Args["record"] = record
	}
	if recordOp != nil {
		q.Args["record_op"] = recordOp
	}
	if pkey != nil {
		q.Args["pkey"] = pkey
	}
	if limit != -1 {
		q.Args["limit"] = limit
	}
	if sort != nil {
		q.Args["sort"] = sort
	}
	if join != nil {
		q.Args["join"] = join
	}
	res, err := p.client.DoQuery(context.Background(), q)
	//res, err := &query.Result{}, errors.New("asdf")
	if err != nil {
		metrics.DBError("transport")
		return res, err
	} else if res.Error != "" {
		metrics.DBError("read")
		return res, errors.New(res.Error)
	}
	return res, err
}

func (p *postgresStore) AddEvent(evt *EventBase) error {
	filter := map[string]interface{}{
		"service_id":          []interface{}{"=", evt.ServiceId},
		"event_type":          []interface{}{"=", evt.EventType},
		"processed_data_hash": []interface{}{"=", evt.ProcessedDataHash},
	}
	res, err := p.Query(query.Filter, "event_base", filter, nil, nil, nil, 1, nil, nil)
	if err != nil {
		metrics.DBError("read")
		return err
	} else if len(res.Return) == 0 {
		//TODO: fix uniqueness constraint
		record := map[string]interface{}{
			"service_id":          evt.ServiceId,
			"event_type":          evt.EventType,
			"event_name":          evt.EventName,
			"processed_data":      evt.ProcessedData,
			"processed_data_hash": evt.ProcessedDataHash,
		}

		res, err = p.Query(query.Set, "event_base", nil, record, nil, nil, -1, nil, nil)
		if err != nil {
			metrics.DBError("write")
			return err
		}
	}
	util.MapDecode(res.Return[0], &evt)
	return nil
}

func (p *postgresStore) AddEvents(evts []EventBase) map[int]error {
	var errs = make(map[int]error)
	for i := range evts {
		err := p.AddEvent(&evts[i])
		if err != nil {
			errs[i] = err
		}
	}
	return errs
}

func (p *postgresStore) AddEventInstance(evt *EventInstance) error {
	filter := map[string]interface{}{
		"raw_data_hash": []interface{}{"=", evt.RawDataHash},
	}
	res, err := p.Query(query.Filter, "event_instance", filter, nil, nil, nil, 1, nil, nil)
	if err != nil {
		metrics.DBError("read")
		return err
	} else if len(res.Return) == 0 {
		//TODO: fix uniqueness constraint
		record := map[string]interface{}{
			"event_base_id":   evt.EventBaseId,
			"event_detail_id": evt.EventDetailId,
			"raw_data":        evt.RawData,
			"raw_data_hash":   evt.RawDataHash,
		}
		res, err = p.Query(query.Set, "event_instance", nil, record, nil, nil, -1, nil, nil)
		if err != nil {
			metrics.DBError("write")
			return err
		}
	}
	util.MapDecode(res.Return[0], &evt)
	return nil
}

func (p *postgresStore) AddEventInstances(evts []EventInstance) map[int]error {
	var errs = make(map[int]error)
	for i := range evts {
		err := p.AddEventInstance(&evts[i])
		if err != nil {
			errs[i] = err
		}
	}
	return errs
}

func (p *postgresStore) AddEventInstancePeriod(evt *EventInstancePeriod) error {
	filter := map[string]interface{}{
		"event_instance_id": []interface{}{"=", evt.EventInstanceId},
		"start_time":        []interface{}{"=", evt.StartTime},
		"end_time":          []interface{}{"=", evt.EndTime},
	}
	record := map[string]interface{}{
		"event_instance_id": evt.EventInstanceId,
		"start_time":        evt.StartTime,
		"updated":           evt.Updated,
		"end_time":          evt.EndTime,
		"count":             evt.Count,
		"counter_json":      evt.CounterJson,
	}
	for {
		res, err := p.Query(query.Filter, "event_instance_period", filter, nil, nil, nil, -1, nil, nil)
		if err != nil {
			metrics.DBError("read")
			return err
		} else if len(res.Return) == 0 {
			//TODO: fix uniqueness constraint
			res, err = p.Query(query.Set, "event_instance_period", nil, record, nil, nil, -1, nil, nil)
			if err != nil {
				metrics.DBError("write")
				return err
			}
		} else {
			var tmp EventInstancePeriod
			util.MapDecode(res.Return[0], &tmp)
			filter["cas_value"] = []interface{}{"=", tmp.CAS}
			record["count"] = tmp.Count + evt.Count
			// TODO: handle error
			record["counter_json"], _ = GlobalRule.Consolidate(tmp.CounterJson, evt.CounterJson)
			record["cas_value"] = tmp.CAS + 1
			res, err = p.Query(query.Update, "event_instance_period", filter, record, nil, nil, -1, nil, nil)
			// if update failed then CAS failed, must retry
			if err != nil {
				continue
			}
		}
		util.MapDecode(res.Return[0], &evt)
		return err
	}
}

func (p *postgresStore) AddEventinstancePeriods(evts []EventInstancePeriod) map[int]error {
	var errs = make(map[int]error)
	for i := range evts {
		err := p.AddEventInstancePeriod(&evts[i])
		if err != nil {
			errs[i] = err
		}
	}
	return errs
}

func (p *postgresStore) AddEventDetail(evt *EventDetail) error {
	filter := map[string]interface{}{
		"processed_detail_hash": []interface{}{"=", evt.ProcessedDetailHash},
	}
	res, err := p.Query(query.Filter, "event_detail", filter, nil, nil, nil, 1, nil, nil)
	if err != nil {
		metrics.DBError("read")
		return err
	} else if len(res.Return) == 0 {
		//TODO: fix uniqueness constraint
		record := map[string]interface{}{
			"raw_detail":            evt.RawDetail,
			"processed_detail":      evt.ProcessedDetail,
			"processed_detail_hash": evt.ProcessedDetailHash,
		}
		res, err = p.Query(query.Set, "event_detail", nil, record, nil, nil, -1, nil, nil)
		if err != nil {
			metrics.DBError("write")
			return err
		}
	}
	util.MapDecode(res.Return[0], &evt)
	return err
}

func (p *postgresStore) AddEventDetails(evts []EventDetail) map[int]error {
	var errs = make(map[int]error)
	for i := range evts {
		err := p.AddEventDetail(&evts[i])
		if err != nil {
			errs[i] = err
		}
	}
	return errs
}
