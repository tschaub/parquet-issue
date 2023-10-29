Test demonstrating an issue with a transformed Parquet file.

## Setup

The test relies on the `parquet-reader` binary from [the C++ package](https://arrow.apache.org/install/).  On macOS, this can be installed with Homebrew:

```shell
brew install apache-arrow
```

## Test

The test is based on a subset of a [Overture Maps](https://github.com/OvertureMaps/data) parquet file.  Only the `sources` and `bbox` columns and a single row are included in the `input.parquet` file.  See the `notes.md` file for details.

Running `make test` will run `main.go` to create the `output.parquet` file and then attempt to read the output file with `parquet-reader`:

```shell
make test
```

With [the latest](https://github.com/apache/arrow/commit/7ef517e31ec3) Go module on macOS 13 arm64, this results in the following:

```shell
parquet-reader output.parquet > /dev/null
Parquet error: Malformed levels. min: 2 max: 2 out of range.  Max Level: 1
make: *** [test] Error 255
```
