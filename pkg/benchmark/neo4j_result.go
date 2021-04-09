package benchmark

import (
	"fmt"
)

// The structure of the Neo4j records is quite verbose with all keys repeated in each record, and yet not convenient either
// because each record is not even a map (so we get neither compactness, nor map-lookup). So this code is designed to
// at least provide compactness in JSON by listing the headers once, and then all records as arrays of arrays.
//
// The structure we get if we use json.Marshal(records) on the results of `SHOW DATABASES`:
//
//  [
//   {
//     "Values":["neo4j","neo4j-core-61e22a5f-2.craigtaverner-orch-0001.neo4j.io:7687","leader","online","online","",true,true],
//     "Keys":["name","address","role","requestedStatus","currentStatus","error","default","systemDefault"]
//   },
//   {
//     "Values":["neo4j","neo4j-core-61e22a5f-1.craigtaverner-orch-0001.neo4j.io:7687","follower","online","online","",true,true],
//     "Keys":["name","address","role","requestedStatus","currentStatus","error","default","systemDefault"]
//   },
//   {
//     "Values":["neo4j","neo4j-core-61e22a5f-3.craigtaverner-orch-0001.neo4j.io:7687","follower","online","online","",true,true],
//     "Keys":["name","address","role","requestedStatus","currentStatus","error","default","systemDefault"]
//   },
//   {
//     "Values":["system","neo4j-core-61e22a5f-2.craigtaverner-orch-0001.neo4j.io:7687","follower","online","online","",false,false],
//     "Keys":["name","address","role","requestedStatus","currentStatus","error","default","systemDefault"]
//   },
//   {
//     "Values":["system","neo4j-core-61e22a5f-1.craigtaverner-orch-0001.neo4j.io:7687","leader","online","online","",false,false],
//     "Keys":["name","address","role","requestedStatus","currentStatus","error","default","systemDefault"]
//   },
//   {
//     "Values":["system","neo4j-core-61e22a5f-3.craigtaverner-orch-0001.neo4j.io:7687","follower","online","online","",false,false],
//     "Keys":["name","address","role","requestedStatus","currentStatus","error","default","systemDefault"]
//   }
//]
//
// The structure we would rather send via JSON:
//
//{
//   "Header":["name","address","role","requestedStatus","currentStatus","error","default","systemDefault"]
//   "Rows": [
//     ["neo4j","neo4j-core-61e22a5f-2.craigtaverner-orch-0001.neo4j.io:7687","leader","online","online","",true,true],
//     ["neo4j","neo4j-core-61e22a5f-1.craigtaverner-orch-0001.neo4j.io:7687","follower","online","online","",true,true],
//     ["neo4j","neo4j-core-61e22a5f-3.craigtaverner-orch-0001.neo4j.io:7687","follower","online","online","",true,true],
//     ["system","neo4j-core-61e22a5f-2.craigtaverner-orch-0001.neo4j.io:7687","follower","online","online","",false,false],
//     ["system","neo4j-core-61e22a5f-1.craigtaverner-orch-0001.neo4j.io:7687","leader","online","online","",false,false],
//     ["system","neo4j-core-61e22a5f-3.craigtaverner-orch-0001.neo4j.io:7687","follower","online","online","",false,false]
//   ]
//}

type Neo4jResult struct {
	Header []string
	Rows   [][]interface{}
}

func NewNeo4jResult(keys []string) *Neo4jResult {
	return &Neo4jResult{keys, [][]interface{}{}}
}

func (r *Neo4jResult) add(values []interface{}) {
	r.Rows = append(r.Rows, values)
}

func (r *Neo4jResult) contains(values []interface{}) bool {
	for _, row := range r.Rows {
		if len(row) == len(values) {
			matches := 0
			for i, v := range values {
				if v == row[i] {
					matches++
				}
			}
			if matches == len(values) {
				return true
			}
		}
	}
	return false
}

// This function will edit the result by removing roles that do not match the filter.
//
// For example the filter:
//
//     map[string]interface{}{"name": "system", "role": "follower"}
//
// Would convert the above example to:
//
//{
//   "Header":["name","address","role","requestedStatus","currentStatus","error","default","systemDefault"]
//   "Rows": [
//     ["system","neo4j-core-61e22a5f-2.craigtaverner-orch-0001.neo4j.io:7687","follower","online","online","",false,false],
//     ["system","neo4j-core-61e22a5f-3.craigtaverner-orch-0001.neo4j.io:7687","follower","online","online","",false,false]
//   ]
//}
func (r *Neo4jResult) FilterResultByRows(filter map[string]interface{}) *Neo4jResult {
	if filter == nil || len(filter) == 0 {
		return r
	}
	newResult := NewNeo4jResult(r.Header)
	headers := r.Header
	columnIndexes := []int{}
	for key := range filter {
		for headerIndex, header := range headers {
			if key == header {
				columnIndexes = append(columnIndexes, headerIndex)
			}
		}
	}
	if len(columnIndexes) == len(filter) {
		for _, row := range r.Rows {
			for _, headerIndex := range columnIndexes {
				header := newResult.Header[headerIndex]
				value := row[headerIndex]
				expected := filter[header]
				if value == expected {
					newResult.add(row)
				}
			}
		}
	}
	return newResult
}

// This function will edit the result by removing columns not specified in the filter.
// If this results in row duplication, duplicates are removed.
//
// For example the filter:
//
//     []string{"name", "role"},
//
// Would convert the above example to:
//
//{
//   "Header":["name","role"]
//   "Rows": [
//     ["neo4j","follower"],
//     ["neo4j","leader"],
//     ["system","follower"],
//     ["system","leader"]
//   ]
//}
func (r *Neo4jResult) FilterResultByColumns(columns []string) *Neo4jResult {
	if columns == nil || len(columns) == 0 {
		return r
	}
	newResult := NewNeo4jResult(columns)
	headers := r.Header
	columnIndexes := []int{}
	exists := false
	for i, c := range columns {
		columnIndexes = append(columnIndexes, -1)
		for x, h := range headers {
			if c == h {
				columnIndexes[i] = x
				exists = true
			}
		}
	}
	if exists {
		for _, row := range r.Rows {
			newRow := []interface{}{}
			for i, coli := range columnIndexes {
				newRow = append(newRow, "")
				if coli >= 0 {
					newRow[i] = row[coli]
				}
			}
			if !newResult.contains(newRow) {
				newResult.add(newRow)
			}
		}
	}
	return newResult
}

func toString(obj interface{}) string {
	return fmt.Sprintf("%v", obj)
}

func Map(vs []interface{}, f func(interface{}) string) []string {
	vsm := make([]string, len(vs))
	for i, v := range vs {
		vsm[i] = f(v)
	}
	return vsm
}
