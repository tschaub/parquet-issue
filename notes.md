The `input.parquet` dataset is a subset of a Parquet file from Overture Maps.


Download the full `data.parquet` file with this:

```shell
curl https://overturemaps-us-west-2.s3.us-west-2.amazonaws.com/release/2023-10-19-alpha.0/theme=buildings/type=building/part-00765-87dd7d19-acc8-4d4f-a5ba-20b407a79638.c000.zstd.parquet -o data.parquet
```

Then, in an environment with the Python `pyarrow` package installed, run the following:

```py
import pyarrow
import pyarrow.parquet as parquet

file = parquet.ParquetFile('data.parquet')
batch = next(file.iter_batches(columns=['sources', 'bbox'], batch_size=1))
table = pyarrow.Table.from_batches([batch])
parquet.write_table(table, 'input.parquet')
```

The resulting `input.parquet` file can be used to demonstrate the problem.
