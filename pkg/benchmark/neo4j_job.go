package benchmark

import (
	"errors"
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
	"log"
	"time"
)

type Neo4jJob struct {
	dbid    string
	neo4j   Neo4j
	running bool
	done    chan struct{}
}

type SessionMaker interface {
	NewQuerySession(neo4j Neo4j, accessMode neo4j.AccessMode) (QuerySession, error)
	NewTimestampMaker() TimestampMaker
}

func nameOf(accessMode neo4j.AccessMode) string {
	switch accessMode {
	case neo4j.AccessModeRead:
		return "read"
	case neo4j.AccessModeWrite:
		return "write"
	default:
		return "INVALID"
	}
}

func NewNeo4jJob(neo4j Neo4j) *Neo4jJob {
	return &Neo4jJob{neo4j.dbid, neo4j, false, make(chan struct{}, 1)}
}

func (n *Neo4jJob) createModel(maker SessionMaker) error {
	accessMode := neo4j.AccessModeWrite
	runner, err := maker.NewQuerySession(n.neo4j, accessMode)
	if err != nil {
		return err
	} else {
		defer runner.Close()
		result, err := runner.RunCypherQuery(accessMode, "MERGE (n:ClientBenchmark) ON CREATE SET n.counter = 0 RETURN n.counter")
		if err != nil {
			return err
		} else {
			if len(result.Rows) != 1 {
				return errors.New(fmt.Sprintf("Expected exactly one 'ClientBenchmark' node in database on '%s' but found %v", n.dbid, len(result.Rows)))
			} else {
				return nil
			}
		}
	}
}

func (n *Neo4jJob) runWorkload(ch chan Message, maker SessionMaker, accessMode neo4j.AccessMode, query string, expected int) {
	countErrors := 0
	maxErrors := 10
	accessModeName := nameOf(accessMode)
	errorMsg := fmt.Sprintf("%s:error", accessModeName)
	runner, err := maker.NewQuerySession(n.neo4j, accessMode)
	if err != nil {
		log.Printf("Failed to create runner for %s workload against '%s': %v", accessModeName, n.dbid, err)
		ch <- Message{errorMsg, n.dbid, -1}
	} else {
		defer runner.Close()
		if !n.running {
			log.Printf("Unexpected found running=false when starting %s workload against '%s' (errors=%d, running=%v)", accessModeName, n.dbid, countErrors, n.running)
			n.running = true
		} else {
			log.Printf("Starting %s workload against '%s' (errors=%d, running=%v)", accessModeName, n.dbid, countErrors, n.running)
		}
		for n.running && countErrors < maxErrors {
			select {
			case <-n.done:
				log.Printf("Received 'done' message - terminating %s workload against '%s'", accessModeName, n.dbid)
				n.running = false
			default:
				time.Sleep(time.Second)
				log.Printf("About to run %s query against '%s'", accessModeName, n.dbid)
				started := time.Now()
				result, err := runner.RunCypherQuery(accessMode, query)
				if err != nil {
					log.Printf(
						"Error running %s query against '%s': %v", accessModeName, n.dbid, err)
					countErrors += 1
					ch <- Message{errorMsg, n.dbid, int64(countErrors)}
				} else if len(result.Rows) != 1 {
					log.Printf("Incorrect number of result rows running %s query against '%s': expected %d rows but got %d", accessModeName, n.dbid, expected, len(result.Rows))
					countErrors += 1
					ch <- Message{errorMsg, n.dbid, int64(countErrors)}
				} else {
					duration := time.Since(started)
					ch <- Message{accessModeName, n.dbid, duration.Milliseconds()}
				}
			}
		}
		log.Printf("Finishing %s workload against '%s' (errors=%d, running=%v)", accessModeName, n.dbid, countErrors, n.running)
	}
}

func (n *Neo4jJob) Check(maker SessionMaker) error {
	session, err := maker.NewQuerySession(n.neo4j, neo4j.AccessModeRead)
	if err != nil {
		return err
	}
	defer session.Close()
	err = session.Check()
	if err != nil {
		return err
	}
	return nil
}

func (n *Neo4jJob) Start(ch chan Message, maker SessionMaker) {
	if !n.running {
		err := n.createModel(maker)
		if err != nil {
			log.Printf("Failed to setup model for '%s': %v", n.dbid, err)
			ch <- Message{"model:error", n.dbid, -1}
		} else {
			n.running = true
			go n.runWorkload(ch, maker, neo4j.AccessModeRead, "MATCH (n:ClientBenchmark) RETURN count(n)", 1)
			go n.runWorkload(ch, maker, neo4j.AccessModeWrite, "MATCH (n:ClientBenchmark) WHERE exists(n.counter) SET n.counter = n.counter + 1 RETURN n.counter", 1)
		}
	}
}

func (n *Neo4jJob) Stop() {
	n.done <- struct{}{}
}

func makeNeo4jResult(obj *interface{}) (*Neo4jResult, error) {
	unboxed, ok := (*obj).([]neo4j.Record)
	if !ok {
		return nil, errors.New("invalid result type from Neo4j query")
	}
	if len(unboxed) == 0 {
		return NewNeo4jResult([]string{}), nil
	}
	result := NewNeo4jResult(unboxed[0].Keys)
	for _, row := range unboxed {
		result.add(row.Values)
	}
	return result, nil
}
