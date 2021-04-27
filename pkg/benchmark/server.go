package benchmark

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
)

type Server struct {
	environment string // for constructing database access URI
	listenPort  int    // The client benchmark server will listen on this port for REST requests
}

const (
	contentType     = "Content-Type"
	contentTypeHTML = "text/html"
	contentTypeText = "text/plain"
	contentTypeJSON = "application/json"
)

func mustReadEnv(key string) string {
	value, ok := os.LookupEnv(key)
	if !ok {
		panic(fmt.Sprintf("%q environment variable not set", key))
	}
	return value
}

func mustReadEnvAsInt(key string) int {
	value := mustReadEnv(key)
	integer, err := strconv.Atoi(value)
	if err != nil {
		panic(fmt.Sprintf("%q environment variable not a valid integer: %s", key, value))
	}
	return integer
}

func NewServer() *Server {
	listen_port := mustReadEnvAsInt("LISTEN_PORT")
	environment := mustReadEnv("ENVIRONMENT")
	if environment == "production" {
		panic(fmt.Sprintf("This service puts a read and write load on databases - and is therefor disabled for production environments"))
	}
	return &Server{environment, listen_port}
}

func (s *Server) handleStringResult(writer http.ResponseWriter, result string, err error, iferr string) {
	if err != nil {
		s.writeErrorMessage(writer, iferr, err)
	} else {
		writer.Header().Set(contentType, contentTypeJSON)
		message, err := resultToJson(result)
		if err != nil {
			s.writeErrorMessage(writer, iferr, err)
		} else {
			writer.Write(message)
		}
	}
}

func resultToJson(text string) ([]byte, error) {
	result := map[string]string{}
	result["result"] = text
	return json.Marshal(result)
}

func makeNeo4jClientResult(clients []*Neo4jJob, workload *Workload) (*Neo4jResult, error) {
	result := NewNeo4jResult([]string{"name", "address", "running", "read", "write"})
	for _, client := range clients {
		read, err := workload.CountsFor(client.dbid, "read")
		if err != nil {
			log.Printf("Failed to get read results for %s: %v", client.dbid, err)
		}
		write, err := workload.CountsFor(client.dbid, "write")
		if err != nil {
			log.Printf("Failed to get write results for %s: %v", client.dbid, err)
		}
		result.add([]interface{}{client.dbid, client.neo4j.neo4jAddress, client.running, read, write})
	}
	return result, nil
}

func (s *Server) handleNeo4jResult(writer http.ResponseWriter, client *Neo4jJob, workload *Workload, err error, iferr string) {
	if err != nil {
		s.writeErrorMessage(writer, iferr, err)
	} else if client != nil {
		s.handleNeo4jResults(writer, []*Neo4jJob{client}, workload, err, iferr)
	} else {
		s.writeError(writer, "Invalid state: no result and no error")
	}
}

func (s *Server) handleNeo4jResults(writer http.ResponseWriter, clients []*Neo4jJob, workload *Workload, err error, iferr string) {
	if err != nil {
		s.writeErrorMessage(writer, iferr, err)
	} else {
		neo4jResult, err := makeNeo4jClientResult(clients, workload)
		s.handleResult(writer, neo4jResult, err, iferr)
	}
}

func (s *Server) handleResult(writer http.ResponseWriter, neo4jResult *Neo4jResult, err error, iferr string) {
	if err != nil {
		s.writeErrorMessage(writer, iferr, err)
	} else {
		if err != nil {
			s.writeErrorMessage(writer, "Failed to create result", err)
			s.writeErrorMessage(writer, "Failed to create result", err)
		} else {
			resultsAsString, err := json.Marshal(neo4jResult)
			if err != nil {
				s.writeErrorMessage(writer, "Failed to create result", err)
			} else {
				writer.Header().Set(contentType, contentTypeJSON)
				writer.Write(resultsAsString)
			}
		}
	}
}

func (s *Server) invalidPath(writer http.ResponseWriter, name string, path string) {
	s.writeError(writer, fmt.Sprintf("invalid path for '%s' request: %s", name, path))
}

func (s *Server) writeError(writer http.ResponseWriter, message string) {
	s.writeErrorMessage(writer, message, nil)
}

func (s *Server) writeErrorMessage(writer http.ResponseWriter, message string, err error) {
	data := map[string]string{}
	data["message"] = message
	if err != nil {
		data["error"] = err.Error()
	}
	writer.WriteHeader(http.StatusBadRequest)
	msgBytes, err2 := json.Marshal(data)
	if err2 == nil {
		writer.Header().Set(contentType, contentTypeJSON)
		writer.Write(msgBytes)
	}
}

func (s *Server) indexHandler() http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set(contentType, contentTypeText)
		fmt.Fprintf(writer, "Commands available for benchmark:\n")
		fmt.Fprintf(writer, "    /                    - show commands\n")
		fmt.Fprintf(writer, "    /neo4j/add/<DBID>    - add workload for database\n")
		fmt.Fprintf(writer, "    /neo4j/remove/<DBID> - add workload for database\n")
		fmt.Fprintf(writer, "    /neo4j/list          - list current database workloads\n")
		fmt.Fprintf(writer, "    /start               - start benchmark\n")
		fmt.Fprintf(writer, "    /stop                - stop benchmark\n")
		fmt.Fprintf(writer, "    /results             - get current results\n")
	}
}

func (s *Server) makeAddress(dbid string) string {
	return fmt.Sprintf("neo4j+s://%s-%s.databases.neo4j.io", dbid, s.environment)
}

func (s *Server) neo4jHandler(workload *Workload) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		username, password, ok := request.BasicAuth()
		if !ok {
			s.writeError(writer, "No basic authentication information provided")
		} else {
			parts := strings.Split(request.URL.Path, "/")
			switch len(parts) {
			case 3:
				verb := parts[2]
				switch verb {
				case "list":
					s.handleNeo4jResults(writer, workload.List(), workload, nil, "")
				default:
					s.invalidPath(writer, parts[1], request.URL.Path)
				}
			case 4:
				verb := parts[2]
				dbid := parts[3]
				address := s.makeAddress(dbid)
				neo4j := NewNeo4j(dbid, address, username, password)
				neo4j_job := NewNeo4jJob(*neo4j)
				switch verb {
				case "add":
					err := workload.Add(neo4j_job)
					s.handleNeo4jResult(writer, neo4j_job, workload, err, "Failed to add workload for neo4j database")

				case "remove":
					err := workload.Remove(neo4j_job)
					s.handleNeo4jResult(writer, neo4j_job, workload, err, "Failed to remove workload for neo4j database")
				case "show":
					err, found := workload.Find(neo4j_job)
					s.handleNeo4jResult(writer, found, workload, err, "Failed show workload for neo4j database")
				default:
					s.invalidPath(writer, parts[1], request.URL.Path)
				}
			default:
				s.invalidPath(writer, parts[1], request.URL.Path)
			}
		}
	}
}

func (s *Server) startHandler(workload *Workload) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		_, _, ok := request.BasicAuth()
		// TODO: actually authenticate
		if !ok {
			s.writeError(writer, "No basic authentication information provided")
		} else {
			result, err := workload.Start()
			s.handleStringResult(writer, result, err, "Failed to start workload")
		}
	}
}

func (s *Server) stopHandler(workload *Workload) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		_, _, ok := request.BasicAuth()
		if !ok {
			s.writeError(writer, "No basic authentication information provided")
		} else {
			result, err := workload.Stop()
			s.handleStringResult(writer, result, err, "Failed to stop workload")
		}
	}
}

func (s *Server) waitHandler(workload *Workload) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		_, _, ok := request.BasicAuth()
		if !ok {
			s.writeError(writer, "No basic authentication information provided")
		} else {
			parts := strings.Split(request.URL.Path, "/")
			switch len(parts) {
			case 3:
				threshold, err := strconv.ParseInt(parts[2], 10, 32)
				if err != nil {
					s.writeErrorMessage(writer, "Failed to parse threshold as integer", err)
				} else {
					result, err := workload.WaitForAtLeast(int(threshold))
					s.handleStringResult(writer, result, err, "Failed to wait for specified number of results")
				}
			default:
				s.invalidPath(writer, parts[1], request.URL.Path)
			}
		}
	}
}

func (s *Server) resultsHandler(workload *Workload) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		_, _, ok := request.BasicAuth()
		if !ok {
			s.writeError(writer, "No basic authentication information provided")
		} else {
			parts := strings.Split(request.URL.Path, "/")
			switch len(parts) {
			case 2:
				result, err := workload.Results()
				s.handleResult(writer, result, err, "Failed to get results")
			case 3:
				switch parts[2] {
				case "table":
					result, err := workload.ResultsTable()
					s.handleResult(writer, result, err, "Failed to get results")
				default:
					dbid := parts[2]
					result, err := workload.ResultsFor(dbid, "read")
					s.handleResult(writer, result, err, "Failed to get results")
				}
			case 4:
				dbid := parts[2]
				verb := parts[3]
				result, err := workload.ResultsFor(dbid, verb)
				s.handleResult(writer, result, err, "Failed to get results")
			default:
				s.invalidPath(writer, parts[1], request.URL.Path)
			}
		}
	}
}

func (s *Server) invalidRequestHandler(path string) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		s.writeError(writer, fmt.Sprintf("Invalid request: %s", path))
	}
}

func (s *Server) Run() {
	sessionMaker := QuerySessionMaker{}
	workload := NewWorkload(&sessionMaker)
	uri := fmt.Sprintf("0.0.0.0:%d", s.listenPort)
	http.HandleFunc("/", s.indexHandler())
	http.HandleFunc("/neo4j/", s.neo4jHandler(workload))
	http.HandleFunc("/start", s.startHandler(workload))
	http.HandleFunc("/stop", s.stopHandler(workload))
	http.HandleFunc("/stats", s.resultsHandler(workload))
	http.HandleFunc("/stats/", s.resultsHandler(workload))
	http.HandleFunc("/wait", s.waitHandler(workload))
	// The certificates are generated by neo4j-init-sidecar which is run as an InitContainer before all normal containers
	log.Fatal(http.ListenAndServe(uri, nil))
}
