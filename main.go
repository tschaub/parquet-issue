package main

import (
	"context"
	"flag"
	"log"
	"os"

	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/parquet"
	"github.com/apache/arrow/go/v14/parquet/compress"
	"github.com/apache/arrow/go/v14/parquet/file"
	"github.com/apache/arrow/go/v14/parquet/pqarrow"
)

func main() {
	var inputName string
	var outputName string
	flag.StringVar(&inputName, "input", "", "input parquet file")
	flag.StringVar(&outputName, "output", "", "output parquet file")
	flag.Parse()

	input, err := os.Open(inputName)
	if err != nil {
		log.Fatal(err)
	}

	fileReader, err := file.NewParquetReader(input)
	if err != nil {
		log.Fatal(err)
	}
	defer fileReader.Close()

	outputSchema := fileReader.MetaData().Schema
	arrowReadProperties := pqarrow.ArrowReadProperties{}

	arrowReader, err := pqarrow.NewFileReader(fileReader, arrowReadProperties, memory.DefaultAllocator)
	if err != nil {
		log.Fatal(err)
	}
	inputManifest := arrowReader.Manifest

	outputManifest, err := pqarrow.NewSchemaManifest(outputSchema, fileReader.MetaData().KeyValueMetadata(), &arrowReadProperties)
	if err != nil {
		log.Fatal(err)
	}

	numFields := len(outputManifest.Fields)
	if numFields != len(inputManifest.Fields) {
		log.Fatalf("unexpected number of fields in the output schema, got %d, expected %d", numFields, len(inputManifest.Fields))
	}

	output, err := os.Create(outputName)
	if err != nil {
		log.Fatal(err)
	}

	writerProperties := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Zstd))
	fileWriter := file.NewParquetWriter(output, outputSchema.Root(), file.WithWriterProps(writerProperties))
	defer fileWriter.Close()

	ctx := context.Background()
	numRowGroups := fileReader.NumRowGroups()
	for rowGroupIndex := 0; rowGroupIndex < numRowGroups; rowGroupIndex += 1 {
		rowGroupReader := arrowReader.RowGroup(rowGroupIndex)
		rowGroupWriter := fileWriter.AppendRowGroup()
		for fieldNum := 0; fieldNum < numFields; fieldNum += 1 {
			arr, err := rowGroupReader.Column(fieldNum).Read(ctx)
			if err != nil {
				log.Fatal(err)
			}
			colWriter, err := pqarrow.NewArrowColumnWriter(arr, 0, int64(arr.Len()), outputManifest, rowGroupWriter, fieldNum)
			if err != nil {
				log.Fatal(err)
			}
			if err := colWriter.Write(ctx); err != nil {
				log.Fatal(err)
			}
		}
	}
}
