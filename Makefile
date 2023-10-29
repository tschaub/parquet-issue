MAKEFLAGS += --warn-undefined-variables
SHELL := /bin/bash -o pipefail -euc
.DEFAULT_GOAL := help

.PHONY: help
help:
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

output.parquet:
	go run main.go -input input.parquet -output output.parquet

.PHONY: test
test: output.parquet ## Test that the converted Parquet file can be read
	parquet-reader output.parquet > /dev/null
