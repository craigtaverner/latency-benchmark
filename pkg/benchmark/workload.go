package benchmark

import (
	"errors"
	"fmt"
	"log"
	"math"
	"sort"
	"strings"
	"time"
)

type TimestampMaker interface {
	CurrentTimestamp() int64
}

type Result struct {
	client     string
	verb       string
	timestamps []int64
	durations  []int64
}

type Results struct {
	timestampMaker TimestampMaker
	results        map[string]Result
}

func (r *Results) Clear() {
	r.results = make(map[string]Result)
}

func (r *Results) Add(verb string, dbid string, value int64) {
	key := fmt.Sprintf("%s:%s", verb, dbid)
	res, ok := r.results[key]
	if !ok {
		res = Result{dbid, verb, []int64{}, []int64{}}
	}
	res.timestamps = append(res.timestamps, r.timestampMaker.CurrentTimestamp())
	res.durations = append(res.durations, value)
	r.results[key] = res
}

func (r *Results) For(dbid string, verb string) Result {
	key := fmt.Sprintf("%s:%s", verb, dbid)
	res, ok := r.results[key]
	if !ok {
		return Result{dbid, verb, []int64{}, []int64{}}
	} else {
		return res
	}
}

func (r *Results) MinMax() (int64, int64) {
	min := int64(math.MaxInt64)
	max := int64(math.MinInt64)
	for _, results := range r.results {
		if len(results.timestamps) > 0 {
			first := results.timestamps[0]
			last := results.timestamps[len(results.timestamps)-1]
			if min > first {
				min = first
			}
			if max < last {
				max = last
			}
		}
	}
	return min, max
}

func (r *Results) Len(dbid string, verb string) int {
	key := fmt.Sprintf("%s:%s", verb, dbid)
	res, ok := r.results[key]
	if !ok {
		return 0
	} else {
		return len(res.durations)
	}
}

type Message struct {
	verb  string
	dbid  string
	value int64
}

type Workload struct {
	runnerMaker SessionMaker
	clients     []*Neo4jJob
	running     bool
	results     Results
	done        chan struct{}
}

func NewWorkload(runnerMaker SessionMaker) *Workload {
	log.Printf("Creating Neo4j Client Benchmark Service")
	return &Workload{runnerMaker: runnerMaker, clients: []*Neo4jJob{}, results: Results{runnerMaker.NewTimestampMaker(), make(map[string]Result)}, done: make(chan struct{})}
}

func (w *Workload) Add(client *Neo4jJob) error {
	log.Printf("Creating Neo4j Client Benchmark Service for %s at %s", client.neo4j.dbid, client.neo4j.neo4jAddress)
	err := client.Check(w.runnerMaker)
	if err != nil {
		return err
	} else {
		w.clients = append(w.clients, client)
		return nil
	}
}

func removeAt(clients []*Neo4jJob, i int) []*Neo4jJob {
	clients[i] = clients[len(clients)-1]
	return clients[:len(clients)-1]
}

func indexOf(clients []*Neo4jJob, client *Neo4jJob) int {
	for i, x := range clients {
		if x.neo4j.dbid == client.neo4j.dbid {
			return i
		}
	}
	return -1
}

func (w *Workload) Remove(client *Neo4jJob) error {
	log.Printf("Removing Neo4j Client Benchmark Service for %s", client.neo4j.dbid)
	found := indexOf(w.clients, client)
	if found < 0 {
		log.Printf("Could not find client for database '%s'", client.neo4j.dbid)
		return errors.New(fmt.Sprintf("Could not find client for database '%s'", client.neo4j.dbid))
	} else {
		w.clients = removeAt(w.clients, found)
		return nil
	}
}

func (w *Workload) List() []*Neo4jJob {
	log.Printf("Listing %d Neo4j Client Benchmark Services", len(w.clients))
	sorted := append([]*Neo4jJob(nil), w.clients...)
	sort.Slice(sorted, func(i, j int) bool {
		return strings.Compare(sorted[i].neo4j.dbid, sorted[j].neo4j.dbid) < 0
	})
	return sorted
}

func (w *Workload) readLoop(ch chan Message) {
	log.Printf("Starting channel read loop")
	for w.running {
		select {
		case msg := <-ch:
			log.Printf("Got message '%s' for '%s': %v", msg.verb, msg.dbid, msg.value)
			switch msg.verb {
			case "read":
				w.results.Add(msg.verb, msg.dbid, msg.value)
			case "write":
				w.results.Add(msg.verb, msg.dbid, msg.value)
			}
		case <-w.done:
			log.Printf("Notified that workload is finished")
			w.running = false
		}
	}
	log.Printf("Exiting channel read loop")
}

func (w *Workload) Start() (string, error) {
	if !w.running {
		ch := make(chan Message, 100)
		w.running = true
		for _, client := range w.clients {
			client.Start(ch, w.runnerMaker)
		}
		w.results.Clear()
		go w.readLoop(ch)
		return "Started", nil
	} else {
		return "", errors.New("Already started")
	}
}

func (w *Workload) maxDurationCount() int {
	max := 0
	for _, verb := range []string{"read", "write"} {
		for _, client := range w.clients {
			count := w.results.Len(client.dbid, verb)
			log.Printf("There were %d durations for %s queries on %s", count, verb, client.dbid)
			if max < count {
				max = count
			}
		}
	}
	return max
}

func (w *Workload) WaitForAtLeast(threshold int) (string, error) {
	log.Printf("Waiting for %d results to be produced", threshold)
	for w.maxDurationCount() < threshold {
		log.Printf("Still have %d < %d results - waiting", w.maxDurationCount(), threshold)
		time.Sleep(time.Second)
	}
	return fmt.Sprintf("%d", w.maxDurationCount()), nil
}

func (w *Workload) Stop() (string, error) {
	if w.running {
		for _, client := range w.clients {
			client.Stop()
		}
		w.done <- struct{}{}
		return "Stopped", nil
	} else {
		return "", errors.New("Already stopped")
	}
}

func (w *Workload) Results() (*Neo4jResult, error) {
	result := NewNeo4jResult([]string{"dbid", "verb", "count"})
	for _, verb := range []string{"read", "write"} {
		for _, client := range w.clients {
			count := w.results.Len(client.dbid, verb)
			result.add([]interface{}{client.dbid, verb, count})
		}
	}
	return result, nil
}

func (w *Workload) ResultsFor(dbid string, verb string) (*Neo4jResult, error) {
	if verb != "read" && verb != "write" {
		return nil, errors.New("Invalid result verb: " + verb)
	}
	result := NewNeo4jResult([]string{"timestamp", "duration"})
	results := w.results.For(dbid, verb)
	for i, duration := range results.durations {
		timestamp := results.timestamps[i]
		result.add([]interface{}{timestamp, duration})
	}
	return result, nil
}

func (w *Workload) CountsFor(dbid string, verb string) (int, error) {
	if verb != "read" && verb != "write" {
		return -1, errors.New("Invalid result verb: " + verb)
	}
	count := w.results.Len(dbid, verb)
	return count, nil
}

func (w *Workload) ResultsTable() (*Neo4jResult, error) {
	columns := []string{"timestamp"}
	for _, client := range w.clients {
		columns = append(columns, fmt.Sprintf("%s:%s", "read", client.dbid))
		columns = append(columns, fmt.Sprintf("%s:%s", "write", client.dbid))
	}
	result := NewNeo4jResult(columns)
	min, max := w.results.MinMax()
	count := int(max - min + 1)
	data := [][]interface{}{}
	add_data := func(column_index int, result Result) {
		for i, timestamp := range result.timestamps {
			duration := result.durations[i]
			offset := int(timestamp - min)
			for len(data) < offset+1 {
				row := make([]interface{}, 2*len(w.clients)+1)
				row[0] = min + int64(len(data))
				for x := 1; x < len(row); x++ {
					row[x] = int64(0)
				}
				data = append(data, row)
			}
			data[offset][column_index] = duration
		}
	}
	for client_index, client := range w.clients {
		add_data(client_index*2+1, w.results.For(client.dbid, "read"))
		add_data(client_index*2+2, w.results.For(client.dbid, "write"))
	}
	find_valid := func(col, i, step int) int64 {
		row := data[i]
		for row[col].(int64) == 0 {
			i += step
			if i >= len(data) || i < 0 {
				break
			}
			row = data[i]
		}
		return row[col].(int64)
	}
	for i := 0; i < count; i++ {
		row := data[i]
		for col, value := range row {
			if col > 0 && value.(int64) == int64(0) {
				if i == 0 {
					row[col] = find_valid(col, i, 1)
				} else if i == count-1 {
					row[col] = find_valid(col, i, -1)
				} else {
					prev := find_valid(col, i, -1)
					next := find_valid(col, i, 1)
					// Weighted average with 90% based on previous to give ramp-up effect
					row[col] = (9*prev + next) / 10
				}
			}
		}
		result.add(row)
	}
	return result, nil
}
