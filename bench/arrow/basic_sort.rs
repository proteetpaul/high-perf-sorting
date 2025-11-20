use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
extern crate arrow;
extern crate clap;
extern crate parquet;

use arrow::array::{RecordBatch, RecordBatchReader, StructArray};
use arrow::compute::{SortOptions, sort_to_indices, take};
use clap::{Parser, arg};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::properties::WriterProperties;

#[derive(Parser)]
struct Args {
    #[arg(short, long)]
    input_file: PathBuf,

    #[arg(short, long)]
    output_file: PathBuf,
}

fn sort_record_batch(batch: &RecordBatch) -> Result<RecordBatch, arrow::error::ArrowError> {
    if batch.num_columns() != 1 {
        panic!("Expected RecordBatch with exactly one column (the StructArray).");
    }
    
    let column = batch.column(0);
    let struct_array = column.as_any().downcast_ref::<StructArray>().unwrap();

    // The key is assumed to be the first field in the StructArray
    let key_array = struct_array.column(0);
    
    let options = Some(SortOptions { descending: false, nulls_first: false });
    let indices = sort_to_indices(key_array, options, None)?;

    // Apply the indices to the original StructArray to get the sorted result
    let sorted_struct_array = take(struct_array, &indices, None)?;
    
    // Recreate the RecordBatch with the sorted StructArray
    let sorted_batch = RecordBatch::try_new(
        batch.schema().clone(), 
        vec![sorted_struct_array]
    )?;

    Ok(sorted_batch)
}

fn run_parquet_sorter(args: Args) -> Result<(), arrow::error::ArrowError> {
    let file = File::open(&args.input_file)?;
    // let file_reader = SerializedFileReader::new(file)?;
    // let row_group_reader = file_reader.get_row_group(0)?;
    let mut builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let num_rows = builder.metadata().file_metadata().num_rows();
    builder = builder.with_batch_size(num_rows as usize);
    let record_batch_reader = builder.build()?;
    let schema = record_batch_reader.schema();

    // let record_batch_reader = arrow_reader.get_record_reader(schema_ref, None)?;
    
    let output_file = File::create(&args.output_file)?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(output_file, schema.clone(), Some(props))?;
    println!("Record batch schema: {}", schema);

    let mut total_sorted_rows = 0;
    let mut num_batches = 0;

    for batch_result in record_batch_reader {
        let batch = batch_result?;
        
        // Apply the sorting logic to the current batch
        let sorted_batch = sort_record_batch(&batch)?;
        
        // Write the sorted batch to the output file
        writer.write(&sorted_batch)?;
        total_sorted_rows += sorted_batch.num_rows();
        num_batches += 1;
    }
    println!("Num batches: {}", num_batches);
    
    // 4. Finalize
    writer.close()?;

    println!("Successfully sorted {} rows.", total_sorted_rows);
    println!("Sorted Parquet file written to: {:?}", args.output_file);

    Ok(())
}

fn main() -> Result<(), arrow::error::ArrowError> {
    let args = Args::parse();
    run_parquet_sorter(args)
}