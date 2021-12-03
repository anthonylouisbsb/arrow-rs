use criterion::{criterion_group, criterion_main, Criterion, black_box};
use arrow::record_batch::RecordBatchReader;
use parquet::file::reader::SerializedFileReader;
use parquet::arrow::{ParquetFileArrowReader, ArrowReader};
use std::sync::{Arc};
use std::fs::File;
use std::env;
use arrow::ipc::RecordBatch;

fn add_benches(c: &mut Criterion) {
    let mut group = c.benchmark_group("parquet_reader_benchmark");

    let batch_size: usize = 65000;

    group.bench_function(
        "Read Int Column",
        |b| {
            b.iter(|| {
                let path_from_env = env::var("ARROW_RUST_PARQUET_BENCHMARK_FILE").expect("ARROW_RUST_PARQUET_BENCHMARK_FILE is not set");
                let file = File::open(path_from_env).unwrap();
                let file_reader = SerializedFileReader::new(file).unwrap();
                let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(file_reader));

                let mut record_batch_reader = arrow_reader
                    .get_record_reader_by_columns(vec![0], batch_size)
                    .expect("The batch reader was not set");

                for maybe_record_batch in record_batch_reader {
                    let record_batch = maybe_record_batch.unwrap();
                    black_box(record_batch);
                }
            })
        },
    );

    group.bench_function(
        "Read BigInt Column",
        |b| {
            b.iter(|| {
                let path_from_env = env::var("ARROW_RUST_PARQUET_BENCHMARK_FILE").expect("ARROW_RUST_PARQUET_BENCHMARK_FILE is not set");
                let file = File::open(path_from_env).unwrap();
                let file_reader = SerializedFileReader::new(file).unwrap();
                let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(file_reader));

                let mut record_batch_reader = arrow_reader
                    .get_record_reader_by_columns(vec![1], batch_size)
                    .unwrap();

                for maybe_record_batch in record_batch_reader {
                    let record_batch = maybe_record_batch.unwrap();
                    black_box(record_batch);
                }
            })
        },
    );

    group.bench_function(
        "Read Varchar Column",
        |b| {
            b.iter(|| {
                let path_from_env = env::var("ARROW_RUST_PARQUET_BENCHMARK_FILE").expect("ARROW_RUST_PARQUET_BENCHMARK_FILE is not set");
                let file = File::open(path_from_env).unwrap();
                let file_reader = SerializedFileReader::new(file).unwrap();
                let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(file_reader));

                let mut record_batch_reader = arrow_reader
                    .get_record_reader_by_columns(vec![2], batch_size)
                    .unwrap();

                for maybe_record_batch in record_batch_reader {
                    let record_batch = maybe_record_batch.unwrap();
                    black_box(record_batch);
                }
            })
        },
    );

    // group.bench_function(
    //     "Read Decimal Column",
    //     |b| {
    //         b.iter(|| {
    //             let path_from_env = env::var("ARROW_RUST_PARQUET_BENCHMARK_FILE").expect("ARROW_RUST_PARQUET_BENCHMARK_FILE is not set");
    //             let file = File::open(path_from_env).unwrap();
    //             let file_reader = SerializedFileReader::new(file).unwrap();
    //             let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(file_reader));
    //
    //             let mut record_batch_reader = arrow_reader
    //                 .get_record_reader_by_columns(vec![3], batch_size)
    //                 .unwrap();
    //
    //             let mut record_batch: arrow::record_batch::RecordBatch;
    //             loop {
    //                 record_batch = record_batch_reader.next().unwrap().unwrap();
    //             }
    //         })
    //     },
    // );
    //
    // group.bench_function(
    //     "Read Struct Column",
    //     |b| {
    //         b.iter(|| {
    //             let path_from_env = env::var("ARROW_RUST_PARQUET_BENCHMARK_FILE").expect("ARROW_RUST_PARQUET_BENCHMARK_FILE is not set");
    //             let file = File::open(path_from_env).unwrap();
    //             let file_reader = SerializedFileReader::new(file).unwrap();
    //             let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(file_reader));
    //
    //             let mut record_batch_reader = arrow_reader
    //                 .get_record_reader_by_columns(vec![4], batch_size)
    //                 .unwrap();
    //
    //             let mut record_batch: arrow::record_batch::RecordBatch;
    //             loop {
    //                 record_batch = record_batch_reader.next().unwrap().unwrap();
    //             }
    //         })
    //     },
    // );
    //
    // group.bench_function(
    //     "Read List of Structs Column",
    //     |b| {
    //         b.iter(|| {
    //             let path_from_env = env::var("ARROW_RUST_PARQUET_BENCHMARK_FILE").expect("ARROW_RUST_PARQUET_BENCHMARK_FILE is not set");
    //             let file = File::open(path_from_env).unwrap();
    //             let file_reader = SerializedFileReader::new(file).unwrap();
    //             let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(file_reader));
    //
    //             let mut record_batch_reader = arrow_reader
    //                 .get_record_reader_by_columns(vec![5], batch_size)
    //                 .unwrap();
    //
    //             let mut record_batch: arrow::record_batch::RecordBatch;
    //             loop {
    //                 record_batch = record_batch_reader.next().unwrap().unwrap();
    //             }
    //         })
    //     },
    // );

    group.finish();
}

criterion_group!(benches, add_benches);
criterion_main!(benches);
