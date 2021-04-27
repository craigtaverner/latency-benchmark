ifeq "$(PROFILE)" "t"
  SHELL := build/bin/mkprofile
else
  SHELL := bash
endif
.ONESHELL:
.SHELLFLAGS := -eu -o pipefail -c
.DELETE_ON_ERROR:
.SECONDEXPANSION:

MAKEFLAGS += --warn-undefined-variables
MAKEFLAGS += --no-builtin-rules

ifeq ($(origin .RECIPEPREFIX), undefined)
  $(error This Make does not support .RECIPEPREFIX. Please use GNU Make 4.0 or later)
endif
.RECIPEPREFIX = >

.PHONY: build run clean check-env

build: latency-benchmark-service

latency-benchmark-service: pkg/benchmark/*.go
> go build -o $@ main.go

run: build check-env
> export LISTEN_PORT=8099
> go run main.go

clean:
> rm -f latency-benchmark-service

check-env:
ifndef ENVIRONMENT
> $(error ENVIRONMENT is undefined - please set this to the Aura environment you wish to benchmark)
endif
