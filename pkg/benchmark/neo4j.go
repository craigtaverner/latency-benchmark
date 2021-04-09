package benchmark

import (
	"errors"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
	"log"
	"time"
)

type QuerySession interface {
	Check() error
	RunCypherQuery(accessMode neo4j.AccessMode, query string) (result *Neo4jResult, err error)
	Close() error
}

type Neo4j struct {
	dbid         string
	database     string
	neo4jAddress string
	username     string
	password     string
}

type Neo4jSession struct {
	neo4j   Neo4j
	session neo4j.Session
}

func NewNeo4j(dbid string, neo4jAddress string, username string, password string) *Neo4j {
	log.Printf("Starting Neo4j Database Client for '%s'", dbid)
	return &Neo4j{dbid, "neo4j", neo4jAddress, username, password}
}

func (s *Neo4jSession) Check() error {
	result, err := s.session.Run("MATCH (n) RETURN count(n)", nil)
	if err != nil {
		return err
	} else if result.Next() {
		return nil
	} else {
		return errors.New("Expected at least one record from counting check query")
	}
}

func (s *Neo4jSession) RunCypherQuery(accessMode neo4j.AccessMode, query string) (result *Neo4jResult, err error) {
	return s.neo4j.runCypherQueryWithColumns(s.session, accessMode, query, []string{})
}

func (s *Neo4jSession) Close() error {
	return s.session.Close()
}

func (n *Neo4j) runCypherQueryWithColumns(session neo4j.Session, accessMode neo4j.AccessMode, query string, columns []string) (result *Neo4jResult, err error) {
	return n.runCypherQueryWithColumnsAndRows(session, accessMode, query, columns, map[string]interface{}{})
}

func (n *Neo4j) runCypherQueryWithColumnsAndRows(session neo4j.Session, accessMode neo4j.AccessMode, query string, columns []string, rows map[string]interface{}) (result *Neo4jResult, err error) {
	log.Printf("About to run the Cypher query '%s' on database %s of deployment %s", query, n.database, n.dbid)

	inTx := session.ReadTransaction
	if accessMode == neo4j.AccessModeWrite {
		inTx = session.WriteTransaction
	}
	records, err := inTx(func(tx neo4j.Transaction) (interface{}, error) {
		var records []neo4j.Record
		results, err := tx.Run(query, nil)
		if err != nil {
			log.Printf("Unable to run the query '%s' on database %s of deployment %s - %v", query, n.database, n.dbid, err)
			return nil, err
		}
		var rec *neo4j.Record
		for results.NextRecord(&rec) {
			records = append(records, *rec)
		}
		return records, nil
	})
	if err != nil {
		return nil, err
	}
	neo4jResult, err := makeNeo4jResult(&records)
	if err != nil {
		return nil, err
	}
	if len(rows) > 0 {
		neo4jResult = neo4jResult.FilterResultByRows(rows)
	}
	if len(columns) > 0 {
		neo4jResult = neo4jResult.FilterResultByColumns(columns)
	}
	return neo4jResult, err
}

type QuerySessionMaker struct {
}

func (m *QuerySessionMaker) NewQuerySession(n Neo4j, accessMode neo4j.AccessMode) (QuerySession, error) {
	configForNeo4j4 := func(conf *neo4j.Config) {
		conf.Log = neo4j.ConsoleLogger(neo4j.INFO)
	}

	driver, err := neo4j.NewDriver(n.neo4jAddress, neo4j.BasicAuth(n.username, n.password, ""), configForNeo4j4)
	if err != nil {
		return nil, err
	}

	sessionConfig := neo4j.SessionConfig{AccessMode: accessMode, DatabaseName: n.database}
	session := driver.NewSession(sessionConfig)
	return &Neo4jSession{n, session}, nil
}

type QueryTimestampMaker struct {
}

func (m *QuerySessionMaker) NewTimestampMaker() TimestampMaker {
	return QueryTimestampMaker{}
}

func (t QueryTimestampMaker) CurrentTimestamp() int64 {
	now := time.Now()
	sec := now.Unix()
	return sec
}
