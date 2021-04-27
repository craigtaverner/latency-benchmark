#!/bin/bash

TABLE_FILE="latency-benchmark.txt"
LISTEN_PORT=8099

# shellcheck disable=SC2034
# shellcheck disable=SC2086
function get_dbid {
  uri=$1
  echo $uri | awk -F '/|-' '{print $3}'
}

function server_uri() {
  echo "http://localhost:$LISTEN_PORT"
}

function test_server() {
  cmd="curl -s -u ignore:ignore $(server_uri)"
  $cmd > /dev/null
}

function server_help() {
  cmd="curl -s -u ignore:ignore $(server_uri)"
  $cmd | sed -e 's/^/    /'
}

# shellcheck disable=SC2034
# shellcheck disable=SC2086
function add_client {
  dbid=$1 ; shift
  password=$1 ; shift
  username="neo4j"
  cmd="curl -s -u $username:$password http://localhost:$LISTEN_PORT/neo4j/add/$dbid"
  $cmd | jq
}

# shellcheck disable=SC2034
# shellcheck disable=SC2086
function remove_client {
  dbid=$1 ; shift
  password=$1 ; shift
  username="neo4j"
  cmd="curl -s -u $username:$password http://localhost:$LISTEN_PORT/neo4j/remove/$dbid"
  $cmd | jq
}

# shellcheck disable=SC2034
# shellcheck disable=SC2086
function show_client {
  dbid=$1 ; shift
  password=$1 ; shift
  username="neo4j"
  cmd="curl -s -u $username:$password http://localhost:$LISTEN_PORT/neo4j/show/$dbid"
  $cmd | jq
}

# shellcheck disable=SC2034
# shellcheck disable=SC2086
function save_client {
  dbid=$1 ; shift
  password=$1 ; shift
  username="neo4j"
  mkdir -p stats
  for stats_type in "read" write
  do
    file="stats/${dbid}_${stats_type}.csv"
    cmd="curl -s -u $username:$password http://localhost:$LISTEN_PORT/stats/$dbid/$stats_type"
    echo "Running command: $cmd"
    $cmd | jq -r '.Header, .Rows[] | @csv' > $file
  done
}

# shellcheck disable=SC2034
# shellcheck disable=SC2086
function save_table {
  mkdir -p stats
  file="stats/all.csv"
  cmd="curl -s -u ignore:ignore http://localhost:$LISTEN_PORT/stats/table"
  $cmd | jq -r '.Header, .Rows[] | @csv' > $file
}

# shellcheck disable=SC2034
# shellcheck disable=SC2086
function show_clients {
  cmd="curl -s -u ignore:ignore http://localhost:$LISTEN_PORT/neo4j/list"
  $cmd | jq -r '.Header, .Rows[] | @csv'
}

# shellcheck disable=SC2034
# shellcheck disable=SC2086
function start_benchmark {
  echo -e "\nStarting benchmark: ..."
  cmd="curl -s -u ignore:ignore http://localhost:$LISTEN_PORT/start"
  $cmd | jq
}

# shellcheck disable=SC2034
# shellcheck disable=SC2086
function stop_benchmark {
  echo -e "\nStopping benchmark: ..."
  cmd="curl -s -u ignore:ignore http://localhost:$LISTEN_PORT/stop"
  $cmd | jq
}

# shellcheck disable=SC2086
function do_all {
  command=$1; shift
  dbids="$@"
  while IFS='$\n' read -r line
  do
    #Test1G_10 2z3BqcZKsQ1_2XxoHMcNiBps3viPIIPs0PFugtowzpA  neo4j+s://d5e64753-firedrillmnt3.databases.neo4j.io
    if [[ "$line" != "#"* ]] && [[ -n "$line" ]] ; then
      if [[ "$line" == *neo4j+s* ]] ; then
        fields=($line)
        name=${fields[0]}
        password=${fields[1]}
        uri=${fields[2]}
        dbid=$(get_dbid $uri)
        if [[ -z "$dbids" ]] ; then
          $command $dbid $password
        else
          for xdbid in $dbids ; do
            if [[ "$xdbid" == "$dbid" ]] ; then
              $command $dbid $password
            fi
          done
        fi
      else
        echo "Invalid line in ${TABLE_FILE}: $line"
      fi
    fi
  done < <(cat "$TABLE_FILE")
}

function list_all {
  cat $TABLE_FILE
}

function usage {
  cat << EOHELP

usage: latency-benchmark.sh <command> <args>

where command is:
  help:       print this help
  add_all:    add all databases defined in $TABLE_FILE
  add:        add one or more databases by DBID
  remove_all: remove all databases defined in $TABLE_FILE
  remove:     remove one or more databases by DBID
  show_all:   show all databases already added
  show:       show one or more databases by DBID
  save_all:   save current measurements for all databases already added
  save:       save current measurements for one or more databases by DBID
  list_all:   list all databases defined in $TABLE_FILE
  start:      start benchmarking
  stop:       stop benchmarking

EOHELP
}

# shellcheck disable=SC2034
function main {
  command=$1 ; shift
  case $command in
  list_all)
    list_all
    ;;
  show_all)
    show_clients
    ;;
  show)
    do_all "show_client" "$@"
    ;;
  add_all)
    do_all "add_client"
    ;;
  add)
    do_all "add_client" "$@"
    ;;
  remove_all)
    do_all "remove_client"
    ;;
  remove)
    do_all "remove_client" "$@"
    ;;
  save_all)
    do_all "save_client"
    save_table
    ;;
  save)
    do_all "save_client" "$@"
    ;;
  start)
    start_benchmark "$@"
    ;;
  stop)
    stop_benchmark "$@"
    ;;
  help)
    usage
    echo "The above commands will be used to access the REST API on the server:"
    server_help
    ;;
  *)
    echo "Unknown command: $command"
    usage
    ;;
  esac
}

if [[ $# -gt 0 ]] ; then
  if test_server ; then
    main "$@"
  else
    echo "Are you sure that a latency-benchmark-service is running at $(server_uri)"
  fi
else
  echo "No command given"
  usage
fi

