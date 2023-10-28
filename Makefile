MAKEFLAGS += --warn-undefined-variables
SHELL := /bin/bash -o pipefail -euc
.DEFAULT_GOAL := help

.PHONY: help
help:
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

input.parquet:
	curl https://overturemaps-us-west-2.s3.us-west-2.amazonaws.com/release/2023-10-19-alpha.0/theme=buildings/type=building/part-00765-87dd7d19-acc8-4d4f-a5ba-20b407a79638.c000.zstd.parquet -o input.parquet

output.parquet: input.parquet
	go run main.go -input input.parquet -output output.parquet

.PHONY: test
test: output.parquet ## Test that the converted Parquet file can be read
	parquet-reader output.parquet > /dev/null
