package benchmark

import (
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

func mockRequest(path string, parameters url.Values) *http.Request {
	remoteAddr := "http://www.example.com"
	Url, _ := url.Parse(remoteAddr)
	Url.Path += path
	Url.RawQuery = parameters.Encode()
	return &http.Request{
		Method:     "GET",
		URL:        Url,
		Header:     make(http.Header),
		RemoteAddr: remoteAddr,
		RequestURI: path,
	}
}

func mockServer(t *testing.T) (Server, *Workload) {
	if os.Setenv("LISTEN_PORT", "8099") != nil {
		assert.Fail(t, "Cannot set environment variable")
	}
	if os.Setenv("ENVIRONMENT", "testenv") != nil {
		assert.Fail(t, "Cannot set environment variable")
	}

	s := NewServer()
	m := TestSessionMaker{}
	workload := NewWorkload(&m)
	return *s, workload
}

func Test_Server(t *testing.T) {
	s, workload := mockServer(t)

	tests := []struct {
		path       string
		statuscode int
		expected   string
	}{
		{path: "/invalid", statuscode: http.StatusBadRequest, expected: `{"message":"Invalid request: /invalid"}`},
		{path: "/", statuscode: http.StatusOK, expected: `Commands available for benchmark:
    /                    - show commands
    /neo4j/add/<DBID>    - add workload for database
    /neo4j/remove/<DBID> - add workload for database
    /neo4j/list          - list current database workloads
    /start               - start benchmark
    /stop                - stop benchmark
    /results             - get current results
`},
		{path: "/neo4j/add", statuscode: http.StatusBadRequest, expected: `{"message":"invalid path for 'neo4j' request: /neo4j/add"}`},
		{path: "/neo4j/add/abc", statuscode: http.StatusOK, expected: `{"Header":["name","address","running","read","write"],"Rows":[["abc","neo4j+s://abc-testenv.databases.neo4j.io",false,0,0]]}`},
		{path: "/neo4j/remove", statuscode: http.StatusBadRequest, expected: `{"message":"invalid path for 'neo4j' request: /neo4j/remove"}`},
		{path: "/neo4j/remove/abc", statuscode: http.StatusOK, expected: `{"Header":["name","address","running","read","write"],"Rows":[["abc","neo4j+s://abc-testenv.databases.neo4j.io",false,0,0]]}`},
		{path: "/neo4j/list", statuscode: http.StatusOK, expected: `{"Header":["name","address","running","read","write"],"Rows":[]}`},
		{path: "/neo4j/add/abc", statuscode: http.StatusOK, expected: `{"Header":["name","address","running","read","write"],"Rows":[["abc","neo4j+s://abc-testenv.databases.neo4j.io",false,0,0]]}`},
		{path: "/neo4j/add/xyz", statuscode: http.StatusOK, expected: `{"Header":["name","address","running","read","write"],"Rows":[["xyz","neo4j+s://xyz-testenv.databases.neo4j.io",false,0,0]]}`},
		{path: "/neo4j/add/123", statuscode: http.StatusOK, expected: `{"Header":["name","address","running","read","write"],"Rows":[["123","neo4j+s://123-testenv.databases.neo4j.io",false,0,0]]}`},
		{path: "/neo4j/list", statuscode: http.StatusOK, expected: `{"Header":["name","address","running","read","write"],"Rows":[["123","neo4j+s://123-testenv.databases.neo4j.io",false,0,0],["abc","neo4j+s://abc-testenv.databases.neo4j.io",false,0,0],["xyz","neo4j+s://xyz-testenv.databases.neo4j.io",false,0,0]]}`},
		{path: "/neo4j/remove/abc", statuscode: http.StatusOK, expected: `{"Header":["name","address","running","read","write"],"Rows":[["abc","neo4j+s://abc-testenv.databases.neo4j.io",false,0,0]]}`},
		{path: "/neo4j/list", statuscode: http.StatusOK, expected: `{"Header":["name","address","running","read","write"],"Rows":[["123","neo4j+s://123-testenv.databases.neo4j.io",false,0,0],["xyz","neo4j+s://xyz-testenv.databases.neo4j.io",false,0,0]]}`},
		{path: "/neo4j/remove/123", statuscode: http.StatusOK, expected: `{"Header":["name","address","running","read","write"],"Rows":[["123","neo4j+s://123-testenv.databases.neo4j.io",false,0,0]]}`},
		{path: "/neo4j/remove/123", statuscode: http.StatusBadRequest, expected: `{"error":"Could not find client for database '123'","message":"Failed to remove workload for neo4j database"}`},
		{path: "/neo4j/list", statuscode: http.StatusOK, expected: `{"Header":["name","address","running","read","write"],"Rows":[["xyz","neo4j+s://xyz-testenv.databases.neo4j.io",false,0,0]]}`},
		{path: "/neo4j/remove/xyz", statuscode: http.StatusOK, expected: `{"Header":["name","address","running","read","write"],"Rows":[["xyz","neo4j+s://xyz-testenv.databases.neo4j.io",false,0,0]]}`},
		{path: "/neo4j/list", statuscode: http.StatusOK, expected: `{"Header":["name","address","running","read","write"],"Rows":[]}`},
		{path: "/start", statuscode: http.StatusOK, expected: `{"result":"Started"}`},
		{path: "/start", statuscode: http.StatusBadRequest, expected: `{"error":"Already started","message":"Failed to start workload"}`},
		{path: "/stop", statuscode: http.StatusOK, expected: `{"result":"Stopped"}`},
		{path: "/stop", statuscode: http.StatusBadRequest, expected: `{"error":"Already stopped","message":"Failed to stop workload"}`},
		{path: "/neo4j/add/abc", statuscode: http.StatusOK, expected: `{"Header":["name","address","running","read","write"],"Rows":[["abc","neo4j+s://abc-testenv.databases.neo4j.io",false,0,0]]}`},
		{path: "/start", statuscode: http.StatusOK, expected: `{"result":"Started"}`},
		{path: "/wait/5", statuscode: http.StatusOK, expected: `{"result":"*?>=5*"}`},
		{path: "/stop", statuscode: http.StatusOK, expected: `{"result":"Stopped"}`},
		{path: "/stats", statuscode: http.StatusOK, expected: `{"Header":["dbid","verb","count"],"Rows":[["abc","read",*?>=1*],["abc","write",*?>=5*]]}`},
		{path: "/stats/abc", statuscode: http.StatusOK, expected: `{"Header":["timestamp","duration"],"Rows":[[*?>0*,1000],[*?>1*,1000],[*?>2*,1000],***]}`},
		{path: "/stats/abc/read", statuscode: http.StatusOK, expected: `{"Header":["timestamp","duration"],"Rows":[[*?>0*,1000],[*?>1*,1000],[*?>2*,1000],***]}`},
		{path: "/stats/abc/write", statuscode: http.StatusOK, expected: `{"Header":["timestamp","duration"],"Rows":[[*?>0*,1000],[*?>1*,1000],[*?>2*,1000],***]}`},
		{path: "/stats/abc/other", statuscode: http.StatusBadRequest, expected: `{"error":"Invalid result verb: other","message":"Failed to get results"}`},
		{path: "/stats/table", statuscode: http.StatusOK, expected: `{"Header":["timestamp","read:abc","write:abc"],"Rows":[[1,*?>=1000*,*?>=1000*],[2,*?>=1000*,*?>=1000*],[3,*?>=1000*,*?>=1000*],***]}`},
	}

	for index, data := range tests {
		testName := fmt.Sprintf("test_%d %s", index+1, data.path)
		t.Run(testName, func(t *testing.T) {
			fields := strings.Split(data.path, "?")
			path := fields[0]
			handler := s.invalidRequestHandler(path)
			parts := strings.Split(path, "/")
			switch parts[1] {
			case "":
				handler = s.indexHandler()
			case "neo4j":
				handler = s.neo4jHandler(workload)
			case "start":
				handler = s.startHandler(workload)
			case "stop":
				handler = s.stopHandler(workload)
			case "wait":
				handler = s.waitHandler(workload)
			case "stats":
				handler = s.resultsHandler(workload)
			}
			parameters := url.Values{}
			println(fields)
			if len(fields) > 1 {
				for _, f := range fields[1:] {
					keyval := strings.Split(f, "=")
					parameters.Add(keyval[0], keyval[1])
				}
			}
			request := mockRequest(path, parameters)
			request.SetBasicAuth("ignored", "secret")
			responseRecorder := httptest.NewRecorder()
			handler(responseRecorder, request)
			response := responseRecorder.Result()
			body, _ := ioutil.ReadAll(response.Body)
			if strings.Contains(data.expected, "*") {
				assertWildcardMatches(t, data.expected, body)
			} else {
				assert.Equal(t, data.expected, string(body))
			}
			assert.Equal(t, data.statuscode, response.StatusCode)
		})
	}
}

func evalExpression(t *testing.T, value int64, prefix string, expression string, comparison func(a, b int64) bool) {
	expected, err := strconv.ParseInt(expression[len(prefix):len(expression)], 10, 64)
	if err != nil {
		t.Fatalf("Expected an integer at '%s': %v", expression, err)
	}
	if comparison(value, expected) {
		log.Printf("Pass: %d %s %d", value, prefix, expected)
	} else {
		t.Fatalf("Failed expression test: %d %s %d", value, prefix, expected)
	}
}

func evalSubmatch(t *testing.T, submatch string, expression string) {
	value, err := strconv.ParseInt(submatch, 10, 64)
	if err != nil {
		t.Fatalf("Expected an integer at '%s': %v", submatch, err)
	}
	if strings.HasPrefix(expression, ">=") {
		evalExpression(t, value, ">=", expression, func(a, b int64) bool { return a >= b })
	} else if strings.HasPrefix(expression, "<=") {
		evalExpression(t, value, "<=", expression, func(a, b int64) bool { return a <= b })
	} else if strings.HasPrefix(expression, ">") {
		evalExpression(t, value, ">", expression, func(a, b int64) bool { return a > b })
	} else if strings.HasPrefix(expression, "<") {
		evalExpression(t, value, "<", expression, func(a, b int64) bool { return a < b })
	}
}

func assertWildcardMatches(t *testing.T, expected string, actual []byte) {
	log.Printf("Comparing '%s' to '%s'", expected, actual)
	compiled := regexp.MustCompile(`(\*{3}|\*\?[<>=\d]*\*)`)
	reg_expected := ""
	prev := 0
	rest := ""
	ops := []string{}
	for _, m := range compiled.FindAllIndex([]byte(expected), -1) {
		part := expected[prev:m[0]]
		expr := expected[m[0]:m[1]]
		rest = expected[m[1]:len(expected)]
		prev = m[1]
		for _, e := range `[]{}` {
			ec := string(e)
			part = strings.ReplaceAll(part, ec, `\`+ec)
		}
		reg_expected += part
		if expr == `***` {
			reg_expected += `.+`
		} else if expr == `*?*` {
			reg_expected += `.+`
		} else {
			reg_expected += `(\d+)`
			op := expr[2 : len(expr)-1]
			ops = append(ops, op)
		}
	}
	reg_expected += rest
	cexp := regexp.MustCompile(reg_expected)
	if cexp.Match(actual) {
		submatches := cexp.FindAllSubmatch(actual, -1)
		for i, submatch := range submatches[0] {
			if i > 0 {
				evalSubmatch(t, string(submatch), ops[i-1])
			}
		}
		log.Printf("Matched '%s' to '%s'", expected, actual)
	} else {
		t.Errorf("Failed to match '%s' to '%s'", expected, actual)
	}
}

type TestQuerySession struct {
}

type TestSessionMaker struct {
}

type TestTimestampMaker struct {
	counter int64
}

func (m *TestSessionMaker) NewQuerySession(neo4j Neo4j, accessMode neo4j.AccessMode) (QuerySession, error) {
	return &TestQuerySession{}, nil
}

func (m *TestSessionMaker) NewTimestampMaker() TimestampMaker {
	return &TestTimestampMaker{}
}

func (r *TestQuerySession) Check() error {
	return nil
}

func (r *TestQuerySession) RunCypherQuery(accessMode neo4j.AccessMode, query string) (*Neo4jResult, error) {
	time.Sleep(time.Second)
	result := NewNeo4jResult([]string{"name"})
	result.add([]interface{}{"value"})
	return result, nil
}

func (r *TestQuerySession) Close() error {
	return nil
}

func (t *TestTimestampMaker) CurrentTimestamp() int64 {
	t.counter += 1
	return t.counter
}
