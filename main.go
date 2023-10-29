package main

import (
	"bytes"
	"context"
	"flag"
	"log"
	"os"
	"strings"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/parquet/file"
	"github.com/apache/arrow/go/v14/parquet/pqarrow"
)

func main() {
	var inputName string
	var outputName string
	flag.StringVar(&inputName, "input", "", "input parquet file")
	flag.StringVar(&outputName, "output", "", "output parquet file")
	flag.Parse()

	inputData, err := makeData()
	if err != nil {
		log.Fatal(err)
	}
	if err := os.WriteFile(inputName, inputData, 0644); err != nil {
		log.Fatal(err)
	}

	outputData, err := copyData(inputData)
	if err != nil {
		log.Fatal(err)
	}

	if err := os.WriteFile(outputName, outputData, 0644); err != nil {
		log.Fatal(err)
	}
}

func copyData(data []byte) ([]byte, error) {
	input := bytes.NewReader(data)
	fileReader, err := file.NewParquetReader(input)
	if err != nil {
		return nil, err
	}
	defer fileReader.Close()

	arrowReadProperties := pqarrow.ArrowReadProperties{}
	arrowReader, err := pqarrow.NewFileReader(fileReader, arrowReadProperties, memory.DefaultAllocator)
	if err != nil {
		return nil, err
	}

	arrowSchema, err := pqarrow.FromParquet(fileReader.MetaData().Schema, &arrowReadProperties, fileReader.MetaData().KeyValueMetadata())
	if err != nil {
		return nil, err
	}

	output := &bytes.Buffer{}
	fileWriter, err := pqarrow.NewFileWriter(arrowSchema, output, nil, pqarrow.DefaultWriterProps())
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	numFields := len(arrowReader.Manifest.Fields)
	numRowGroups := fileReader.NumRowGroups()
	for rowGroupIndex := 0; rowGroupIndex < numRowGroups; rowGroupIndex += 1 {
		rowGroupReader := arrowReader.RowGroup(rowGroupIndex)
		fileWriter.NewRowGroup()
		for fieldNum := 0; fieldNum < numFields; fieldNum += 1 {
			arr, err := rowGroupReader.Column(fieldNum).Read(ctx)
			if err != nil {
				return nil, err
			}
			if err := fileWriter.WriteColumnChunked(arr, 0, int64(arr.Len())); err != nil {
				return nil, err
			}
		}
	}
	if err := fileWriter.Close(); err != nil {
		return nil, err
	}

	return output.Bytes(), nil
}

func makeData() ([]byte, error) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "source", Nullable: true, Type: arrow.StructOf(
			arrow.Field{Name: "dataset", Nullable: true, Type: arrow.BinaryTypes.String},
			arrow.Field{Name: "confidence", Nullable: true, Type: arrow.PrimitiveTypes.Float64},
		)},
		{Name: "bbox", Nullable: false, Type: arrow.StructOf(
			arrow.Field{Name: "minx", Nullable: true, Type: arrow.PrimitiveTypes.Float64},
		)},
	}, nil)

	record, _, err := array.RecordFromJSON(memory.DefaultAllocator, schema, strings.NewReader(`[
		{
			"source": {
				"dataset": "test"
				"confidence": 100
			}
			"bbox": {
				"minx": -180
			}
		}
	]`))
	if err != nil {
		return nil, err
	}

	output := &bytes.Buffer{}
	writer, err := pqarrow.NewFileWriter(schema, output, nil, pqarrow.DefaultWriterProps())
	if err != nil {
		return nil, err
	}

	if err := writer.Write(record); err != nil {
		return nil, err
	}

	if err := writer.Close(); err != nil {
		return nil, err
	}

	return output.Bytes(), nil
}
