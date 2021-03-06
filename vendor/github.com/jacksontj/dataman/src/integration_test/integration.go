package integrationtest

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/jacksontj/dataman/src/query"
	"github.com/jacksontj/dataman/src/router_node"
	"github.com/jacksontj/dataman/src/router_node/metadata"
	"github.com/jacksontj/dataman/src/storage_node"
	"github.com/jacksontj/dataman/src/task_node"
)

type Data map[string]map[string][]map[string]interface{}

type Queries []*query.Query

// For this we assume these are empty and we can do whatever we want to them!
func RunIntegrationTests(t *testing.T, task *tasknode.TaskNode, router *routernode.RouterNode, datasource *storagenode.DatasourceInstance) {

	// Find all tests
	files, err := ioutil.ReadDir("tests")
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {
		if !file.IsDir() {
			continue
		}
		// TODO: subtest stuff
		t.Run(file.Name(), func(t *testing.T) {
			runIntegrationTest(file.Name(), t, task, router, datasource)
		})
	}
}

func runIntegrationTest(testDir string, t *testing.T, task *tasknode.TaskNode, router *routernode.RouterNode, datasource *storagenode.DatasourceInstance) {
	// Load the various files
	schema := make(map[string]*metadata.Database)
	schemaBytes, err := ioutil.ReadFile(path.Join("tests", testDir, "/schema.json"))
	if err != nil {
		t.Fatalf("Unable to read schema for test %s: %v", testDir, err)
	}
	if err := json.Unmarshal(schemaBytes, &schema); err != nil {
		t.Fatalf("Unable to load schema for test %s: %v", testDir, err)
	}

	// Load the schema
	for _, database := range schema {
		if err := task.EnsureExistsDatabase(context.Background(), database); err != nil {
			t.Fatalf("Unable to ensureSchema in test %s for database %s: %v", testDir, database.Name, err)
		}
	}

	// Block the router waiting on an update from the tasknode
	router.FetchMeta()

	// Load data
	data := make(Data)
	dataString, err := ioutil.ReadFile(path.Join("tests", testDir, "/data.json"))
	if err != nil {
		t.Fatalf("Unable to read data for test %s: %v", testDir, err)
	}
	if err := json.Unmarshal([]byte(dataString), &data); err != nil {
		t.Fatalf("Unable to load data for test %s: %v", testDir, err)
	}

	for databaseName, collectionMap := range data {
		for collectionName, recordList := range collectionMap {
			for _, record := range recordList {
				result := router.HandleQuery(context.Background(), &query.Query{
					Type: query.Insert,
					Args: map[string]interface{}{
						"db":         databaseName,
						"collection": collectionName,
						"record":     record,
					},
				})
				if result.ValidationError != nil {
					t.Fatalf("Valdiation error loading data into %s.%s: %v", databaseName, collectionName, result.ValidationError)
				}
				if result.Error != "" {
					t.Fatalf("Error loading data into %s.%s: %v", databaseName, collectionName, result.Error)
				}
			}
		}
	}
	testsDir := path.Join("tests", testDir, "/")

	walkFunc := func(fpath string, info os.FileInfo, err error) error {
		// If its a directory, skip it-- we'll let something else grab it
		if !info.IsDir() {
			return nil
		}

		// if this is a test directory it must have a query.json file
		// Load the query
		q := &query.Query{}
		queryBytes, err := ioutil.ReadFile(path.Join(fpath, "query.json"))
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			t.Fatalf("Unable to read queryBytes for test %s.%s: %v", testDir, info.Name(), err)
		}
		if err := json.Unmarshal([]byte(queryBytes), &q); err != nil {
			t.Fatalf("Unable to load queries for test %s.%s: %v", testDir, info.Name(), err)
		}

		relFilePath, err := filepath.Rel(testsDir, fpath)
		if err != nil {
			t.Fatalf("Error getting relative path? Shouldn't be possible: %v", err)
		}
		t.Run(relFilePath, func(t *testing.T) {

			// Run the query
			result := router.HandleQuery(context.Background(), q)

			// Check result

			// write out results
			resultPath := path.Join(fpath, "result.json")
			resultBytes, _ := json.MarshalIndent(result, "", "  ")
			ioutil.WriteFile(resultPath, resultBytes, 0644)

			// compare against baseline if it exists
			baselinePath := path.Join(fpath, "baseline.json")
			baselineResultBytes, err := ioutil.ReadFile(baselinePath)
			if err != nil {
				t.Skip("No baseline.json found, skipping comparison")
			} else {
				baselineResultBytes = bytes.TrimSpace(baselineResultBytes)
				resultBytes = bytes.TrimSpace(resultBytes)
				if !bytes.Equal(baselineResultBytes, resultBytes) {
					t.Fatalf("Mismatch of results and baseline!")
				}
			}
		})
		return nil
	}

	if err := filepath.Walk(testsDir, walkFunc); err != nil {
		t.Errorf("Error walking: %v", err)
	}

	// Assuming we finished properly, lets remove the things we added
	for _, database := range schema {
		if err := task.EnsureDoesntExistDatabase(context.Background(), database.Name); err != nil {
			t.Fatalf("Unable to remove in test %s for database %s: %v", testDir, database.Name, err)
		}
	}

	// Block the router waiting on an update from the tasknode
	router.FetchMeta()

}
