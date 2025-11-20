extern crate clap;
extern crate arrow;
extern crate parquet;

use std::{convert::TryInto, fs::File, io::Read, path::PathBuf, sync::Arc};

use clap::Parser;
use arrow::{
    array::{ArrayRef, FixedSizeBinaryArray, StructArray, UInt64Array},
    datatypes::{DataType, Field, Fields, Schema},
    record_batch::RecordBatch,
};
use parquet::{
    arrow::ArrowWriter,
    file::properties::WriterProperties,
};

const KEY_SIZE: usize = 8;

/// Reads fixed-size records (key-value pairs) from a binary file, 
/// converts them to an Arrow StructArray, and writes the result to a Parquet file.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    input: PathBuf,

    #[arg(short, long)]
    output: PathBuf,

    #[arg(short, long)]
    value_size: usize,
}

fn read_records(args: &Args) -> Vec<u8> {
    println!("Reading records from: {:?}", args.input);
    let mut file = File::open(&args.input).expect("Failed to open input file");
    let mut buffer = Vec::new();
    let res = file.read_to_end(&mut buffer);
    if res.is_err() {
        panic!("Failed to read from input file");
    }

    let record_size = KEY_SIZE + args.value_size;
    if buffer.len() % record_size != 0 {
        panic!("File size is not a multiple of the record size");
    }
    buffer
}

fn run_conversion(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    let key_data_type = DataType::UInt64;
    let value_data_type = DataType::FixedSizeBinary(args.value_size as i32);

    let key_field = Field::new("key", key_data_type.clone(), false);
    let value_field = Field::new("value", value_data_type.clone(), false);

    let struct_fields = Fields::from(vec![key_field, value_field]);
    let schema = Arc::new(Schema::new(vec![
        Field::new("record", DataType::Struct(struct_fields.clone()), false),
    ]));
    
    let input_data = read_records(&args);
    let record_size = KEY_SIZE + args.value_size;
    let num_records = input_data.len() / record_size;
    let mut key_buffers = Vec::<u64>::with_capacity(num_records);
    let mut value_buffers = Vec::with_capacity(num_records * args.value_size);

    for i in 0..num_records {
        let start = i * record_size;
        let x = &input_data[start..start+KEY_SIZE];
        let key = u64::from_le_bytes(x.try_into().unwrap());
        key_buffers.push(key);

        // Extract Value segment
        let value_start = start + KEY_SIZE;
        let value_end = start + KEY_SIZE + args.value_size;
        value_buffers.extend_from_slice(&input_data[value_start..value_end]);
    }
    
    let key_array = UInt64Array::try_new(key_buffers.into(), None)?;
    let value_array = FixedSizeBinaryArray::try_new(args.value_size as i32, value_buffers.into(), None)?;
    let struct_array = StructArray::new(
        struct_fields,
        vec![Arc::new(key_array) as ArrayRef, Arc::new(value_array) as ArrayRef],
        None,
    );

    let batch = RecordBatch::try_new(
        schema.clone(), 
        vec![Arc::new(struct_array) as ArrayRef]
    )?;
    
    println!("RecordBatch created: {} rows", batch.num_rows());
    println!("Writing Parquet file to: {:?}", args.output);

    let file = File::create(args.output)?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
    
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    run_conversion(args)
}