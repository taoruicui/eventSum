package eventsum

import (
	"time"

	"context"
	"encoding/json"
	log "github.com/ContextLogic/eventsum/log"
	"github.com/jacksontj/dataman/src/client"
	"github.com/jacksontj/dataman/src/client/direct"
	"github.com/jacksontj/dataman/src/query"
	"github.com/jacksontj/dataman/src/storage_node"
	"github.com/jacksontj/dataman/src/storage_node/metadata"
	"github.com/pkg/errors"
	"io/ioutil"
)

type dataStore interface {
	Query(typ query.QueryType,
		collection string,
		filter interface{},
		record map[string]interface{},
		recordOp map[string]interface{},
		pkey map[string]interface{},
		limit int,
		sort []string,
		join []interface{}) (*query.Result, error)
	AddEvent(evt *eventBase) error
	AddEvents(evts []eventBase) error
	AddEventInstance(evt *eventInstance) error
	AddEventInstances(evts []eventInstance) error
	AddEventInstancePeriod(evt *eventInstancePeriod) error
	AddEventinstancePeriods(evts []eventInstancePeriod) error
	AddEventDetail(evt *eventDetail) error
	AddEventDetails(evts []eventDetail) error
}

type postgresStore struct {
	client       *datamanclient.Client
	log          *log.Logger
	timeInterval int
}

/* MODELS CORRESPONDING TO DATABASE TABLES */

type eventBase struct {
	Id                int64       `mapstructure:"_id"`
	ServiceId         int         `mapstructure:"service_id"`
	EventType         string      `mapstructure:"event_type"`
	EventName         string      `mapstructure:"event_name"`
	ProcessedData     interface{} `mapstructure:"processed_data"`
	ProcessedDataHash string      `mapstructure:"processed_data_hash"`
}

type eventInstance struct {
	Id            int64       `mapstructure:"_id"`
	EventBaseId   int64       `mapstructure:"event_base_id"`
	EventDetailId int64       `mapstructure:"event_detail_id"`
	RawData       interface{} `mapstructure:"raw_data"`
	RawDataHash   string      `mapstructure:"raw_data_hash"`

	// ignored fields, used internally
	ProcessedDataHash   string
	ProcessedDetailHash string
}

type eventInstancePeriod struct {
	Id              int64                  `mapstructure:"_id"`
	EventInstanceId int64                  `mapstructure:"event_instance_id"`
	StartTime       time.Time              `mapstructure:"start_time"`
	EndTime         time.Time              `mapstructure:"end_time"`
	Updated         time.Time              `mapstructure:"updated"`
	Count           int                    `mapstructure:"count"`
	CounterJson     map[string]interface{} `mapstructure:"counter_json"`
	CAS int `mapstructure:"cas_value"`

	// ignored fields, used internally
	RawDataHash         string
	ProcessedDetailHash string
}

type eventDetail struct {
	Id                  int64       `mapstructure:"_id"`
	RawDetail           interface{} `mapstructure:"raw_detail"`
	ProcessedDetail     interface{} `mapstructure:"processed_detail"`
	ProcessedDetailHash string      `mapstructure:"processed_detail_hash"`
}

// Create a new dataStore
func newDataStore(conf eventsumConfig, log *log.Logger) dataStore {
	// Create a connection to Postgres Database through Dataman

	storagenodeConfig, err := storagenode.DatasourceInstanceConfigFromFile(conf.DataSourceInstance)
	if err != nil {
		log.App.Fatalf("Error loading config: %v", err)
	}

	// Load meta
	meta := &metadata.Meta{}
	metaBytes, err := ioutil.ReadFile(conf.DataSourceSchema)
	if err != nil {
		log.App.Fatalf("Error loading schema: %v", err)
	}
	err = json.Unmarshal([]byte(metaBytes), meta)
	if err != nil {
		log.App.Fatalf("Error loading meta: %v", err)
	}

	// TODO: remove
	storagenodeConfig.SkipProvisionTrim = true

	transport, err := datamandirect.NewStaticDatasourceInstanceTransport(storagenodeConfig, meta)
	if err != nil {
		log.App.Fatalf("Error NewStaticDatasourceInstanceClient: %v", err)
	}

	client := &datamanclient.Client{Transport: transport}
	return &postgresStore{
		client:       client,
		log:          log,
		timeInterval: conf.TimeInterval,
	}
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
			"db":             "event_sum",
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
	if err != nil {
		p.log.App.Panic(err)
	} else if res.Error != "" {
		return res, errors.New(res.Error)
	}
	return res, err
}

func (p *postgresStore) AddEvent(evt *eventBase) error {
	filter := map[string]interface{}{
		"service_id":          []interface{}{"=", evt.ServiceId},
		"event_type":          []interface{}{"=", evt.EventType},
		"processed_data_hash": []interface{}{"=", evt.ProcessedDataHash},
	}
	res, err := p.Query(query.Filter, "event_base", filter, nil, nil, nil, 1, nil, nil)
	if err != nil {
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
			return err
		}
	}
	mapDecode(res.Return[0], &evt)
	return nil
}

func (p *postgresStore) AddEvents(evts []eventBase) error {
	for i := range evts {
		err := p.AddEvent(&evts[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *postgresStore) AddEventInstance(evt *eventInstance) error {
	filter := map[string]interface{}{
		"raw_data_hash": []interface{}{"=", evt.RawDataHash},
	}
	res, err := p.Query(query.Filter, "event_instance", filter, nil, nil, nil, 1, nil, nil)
	if err != nil {
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
			return err
		}
	}
	mapDecode(res.Return[0], &evt)
	return nil
}

func (p *postgresStore) AddEventInstances(evts []eventInstance) error {
	for i := range evts {
		err := p.AddEventInstance(&evts[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *postgresStore) AddEventInstancePeriod(evt *eventInstancePeriod) error {
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
			return err
		} else if len(res.Return) == 0 {
			//TODO: fix uniqueness constraint
			res, err = p.Query(query.Set, "event_instance_period", nil, record, nil, nil, -1, nil, nil)
			if err != nil {
				return err
			}
		} else {
			var tmp eventInstancePeriod
			mapDecode(res.Return[0], &tmp)
			filter["cas_value"] = []interface{}{"=", tmp.CAS}
			record["count"] = tmp.Count + evt.Count
			record["counter_json"], _ = globalRule.Consolidate(tmp.CounterJson, evt.CounterJson)
			record["cas_value"] = tmp.CAS + 1
			res, err = p.Query(query.Update, "event_instance_period", filter, record, nil, nil, -1, nil, nil)
			// if update failed then CAS failed, must retry
			if err != nil {
				continue
			}
		}
		mapDecode(res.Return[0], &evt)
		return err
	}
}

func (p *postgresStore) AddEventinstancePeriods(evts []eventInstancePeriod) error {
	for i := range evts {
		err := p.AddEventInstancePeriod(&evts[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *postgresStore) AddEventDetail(evt *eventDetail) error {
	filter := map[string]interface{}{
		"processed_detail_hash": []interface{}{"=", evt.ProcessedDetailHash},
	}
	res, err := p.Query(query.Filter, "event_detail", filter, nil, nil, nil, 1, nil, nil)
	if err != nil {
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
			return err
		}
	}
	mapDecode(res.Return[0], &evt)
	return err
}

func (p *postgresStore) AddEventDetails(evts []eventDetail) error {
	for i := range evts {
		err := p.AddEventDetail(&evts[i])
		if err != nil {
			return err
		}
	}
	return nil
}
