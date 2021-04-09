package benchmark

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func Test_Neo4jResult(t *testing.T) {

	tests := []struct {
		name       string
		length     int
		original   string
		expected   string
		filterCols []string
		filterRows map[string]interface{}
	}{
		{
			name:     "Simple",
			length:   1,
			original: `[{"Values":["v"],"Keys":["k"]}]`,
			expected: `{"Header":["k"],"Rows":[["v"]]}`,
		},
		{
			name:       "Filtered By Columns",
			length:     2,
			original:   `[{"Keys":["a","b","c","d"],"Values":["v1",1,true,-1]},{"Keys":["a","b","c","d"],"Values":["v2",1,false,-1]},{"Keys":["a","b","c","d"],"Values":["v3",2,true,-1]}]`,
			expected:   `{"Header":["d","b"],"Rows":[[-1,1],[-1,2]]}`,
			filterCols: []string{"d", "b"},
		},
		{
			name:       "Filtered By Rows",
			length:     2,
			original:   `[{"Keys":["a","b","c","d"],"Values":["v1",1,true,-1]},{"Keys":["a","b","c","d"],"Values":["v2",1,false,-1]},{"Keys":["a","b","c","d"],"Values":["v3",2,true,-1]}]`,
			expected:   `{"Header":["a","b","c","d"],"Rows":[["v1",1,true,-1],["v3",2,true,-1]]}`,
			filterRows: map[string]interface{}{"c": true, "d": -1},
		},
		{
			name:   "SHOW DATABASES",
			length: 6,
			original: `
				[
				  {
					"Values":["neo4j","neo4j-core-61e22a5f-2.craigtaverner-orch-0001.neo4j.io:7687","leader","online","online","",true,true],
					"Keys":["name","address","role","requestedStatus","currentStatus","error","default","systemDefault"]
				  },
				  {
					"Values":["neo4j","neo4j-core-61e22a5f-1.craigtaverner-orch-0001.neo4j.io:7687","follower","online","online","",true,true],
					"Keys":["name","address","role","requestedStatus","currentStatus","error","default","systemDefault"]
				  },
				  {
					"Values":["neo4j","neo4j-core-61e22a5f-3.craigtaverner-orch-0001.neo4j.io:7687","follower","online","online","",true,true],
					"Keys":["name","address","role","requestedStatus","currentStatus","error","default","systemDefault"]
				  },
				  {
					"Values":["system","neo4j-core-61e22a5f-2.craigtaverner-orch-0001.neo4j.io:7687","follower","online","online","",false,false],
					"Keys":["name","address","role","requestedStatus","currentStatus","error","default","systemDefault"]
				  },
				  {
					"Values":["system","neo4j-core-61e22a5f-1.craigtaverner-orch-0001.neo4j.io:7687","leader","online","online","",false,false],
					"Keys":["name","address","role","requestedStatus","currentStatus","error","default","systemDefault"]
				  },
				  {
					"Values":["system","neo4j-core-61e22a5f-3.craigtaverner-orch-0001.neo4j.io:7687","follower","online","online","",false,false],
					"Keys":["name","address","role","requestedStatus","currentStatus","error","default","systemDefault"]
				  }
				]`,
			expected: `
				{
				   "Header":["name","address","role","requestedStatus","currentStatus","error","default","systemDefault"],
				   "Rows": [
					 ["neo4j","neo4j-core-61e22a5f-2.craigtaverner-orch-0001.neo4j.io:7687","leader","online","online","",true,true],
					 ["neo4j","neo4j-core-61e22a5f-1.craigtaverner-orch-0001.neo4j.io:7687","follower","online","online","",true,true],
					 ["neo4j","neo4j-core-61e22a5f-3.craigtaverner-orch-0001.neo4j.io:7687","follower","online","online","",true,true],
					 ["system","neo4j-core-61e22a5f-2.craigtaverner-orch-0001.neo4j.io:7687","follower","online","online","",false,false],
					 ["system","neo4j-core-61e22a5f-1.craigtaverner-orch-0001.neo4j.io:7687","leader","online","online","",false,false],
					 ["system","neo4j-core-61e22a5f-3.craigtaverner-orch-0001.neo4j.io:7687","follower","online","online","",false,false]
				   ]
				}`,
		},
	}

	trimmer := strings.NewReplacer("\t", "", "\n", "", "\r", "", " ", "")

	for index, data := range tests {
		t.Run(fmt.Sprintf("test_%d %s", index+1, data.name), func(t *testing.T) {
			// First convert the JSON formatted text to collection of Neo4j record-like structs
			var test []TestRecord
			err := json.Unmarshal([]byte(data.original), &test)
			assert.Nil(t, err)
			assert.NotNil(t, test)

			// Check that the original JSON was well structured by converting back to JSON text
			bytes, err := json.Marshal(test)
			assert.Nil(t, err)
			trimmed := trimmer.Replace(data.original)
			assert.Equal(t, len(trimmed), len(string(bytes)))

			// Convert the neo4j-like data into the new table format
			results := NewNeo4jResult(test[0].Keys)
			for _, row := range test {
				results.add(row.Values)
			}

			// Convert the new structure back into JSON formatted text
			bytes, err = json.Marshal(results)
			assert.Nil(t, err)

			// Run the row and column filtering on the new table format data
			var result Neo4jResult
			err = json.Unmarshal(bytes, &result)
			assert.Nil(t, err)
			result = *result.FilterResultByRows(data.filterRows)
			result = *result.FilterResultByColumns(data.filterCols)
			assert.Equal(t, data.length, len(result.Rows))

			// Convert the filtered data back into JSON string for final check with expected
			bytes, err = json.Marshal(result)
			assert.Nil(t, err)
			trimmed = trimmer.Replace(data.expected)
			assert.Equal(t, trimmed, string(bytes))
		})
	}
}

type TestRecord struct {
	// Values contains all the values in the record.
	Values []interface{}
	// Keys contains names of the values in the record.
	// Should not be modified. Same instance is used for all records within the same result.
	Keys []string
}
