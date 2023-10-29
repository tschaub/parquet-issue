Test demonstrating an issue with a transformed Parquet file.

## Setup

The test relies on the `parquet-reader` binary from [the C++ package](https://arrow.apache.org/install/).  On macOS, this can be installed with Homebrew:

```shell
brew install apache-arrow
```

## Test

The test is based on a stripped down subset of an [Overture Maps](https://github.com/OvertureMaps/data) parquet file.  The `main.go` file generates an `input.parquet` file with two logical columns and a single row.  This `input.parquet` file can be read successfully with the `parquet-reader`.

The `main.go` file copies the `input.parquet` data and writes a file called `output.parquet`.  This is the file that cannot be read by the `parquet-reader`.

Running `make test` will run `main.go` to create the `input.parquet` and `output.parquet` files and then attempt to read both with `parquet-reader`:

```shell
make test
```

With [the latest](https://github.com/apache/arrow/commit/7ef517e31ec3) Go module on macOS 13 arm64, this results in the following:

```shell
go run main.go -input input.parquet -output output.parquet
parquet-reader input.parquet > /dev/null
parquet-reader output.parquet > /dev/null
Parquet error: Malformed levels. min: 2 max: 2 out of range.  Max Level: 1
```
