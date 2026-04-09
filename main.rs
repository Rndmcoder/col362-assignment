// // done reading
// use anyhow::{Context, Result};
// use clap::Parser;
// use common::query::Query;
// use db_config::DbContext;
// use std::{io::{BufRead, BufReader, Read, Write}, string};

// use crate::{
//     cli::CliOptions,
//     io_setup::{setup_disk_io, setup_monitor_io},
// };
// use giveoutput::Executor;
// use once_cell::sync::Lazy;
// use std::sync::Mutex;

// pub static FILE_ID: Lazy<Mutex<String>> = Lazy::new(|| {
//     Mutex::new(String::new())
// });

// mod cli;
// mod io_setup;
// mod giveoutput;

// fn db_main() -> Result<()> {
//     let cli_options = CliOptions::parse();

//     // Use the ctx to the tables and stats
//     let ctx = DbContext::load_from_file(cli_options.get_config_path())?;
//     // println!("context path is {:?}",cli_options.get_config_path());
//     for table_spec in ctx.get_table_specs() {
//         println!("Table: {}", table_spec.name);
//         println!("File id: {}", table_spec.file_id);
//         for column_spec in &table_spec.column_specs {
//             println!(
//                 "\tColumn: {} ({:?})",
//                 column_spec.column_name, column_spec.data_type
//             );
//         }
//         println!();
//     }

//     // Setups and provides handler to talk with disk and monitor
//     let (disk_in, mut disk_out) = setup_disk_io();
//     let (monitor_in, mut monitor_out) = setup_monitor_io();

//     // Use buffered reader to read lines easier (buffers input/ output to some stream)
//     let mut disk_buf_reader = BufReader::new(disk_in);
//     let mut monitor_buf_reader = BufReader::new(monitor_in);

//     // Temporary variable to read a line of input
//     let mut input_line = String::new();

//     // Read query form monitor-> monitor probably only sends just the query
//     monitor_buf_reader.read_line(&mut input_line)?;
//     let query: Query = serde_json::from_str(&input_line).unwrap();
//     println!("Input query is: {:#?}", query); // # is for the pretty print

//     // Interacting with with Disk

//     // Get block size
//     // the get block-size is a command to the disc
//     disk_out.write_all("get block-size\n".as_bytes())?;
//     disk_out.flush()?;

//     input_line.clear();
//     disk_buf_reader.read_line(&mut input_line)?;
//     let block_size: u64 = input_line.trim().parse()?;

//     println!("block size is {}", block_size);

//     disk_out.write_all("get block 0 1\n".as_bytes())?;
//     disk_out.flush()?;

//     let mut buf = vec![0u8; block_size as usize];
//     disk_buf_reader.read_exact(&mut buf)?;

//     println!(
//         "First few bytes of block 0 contains {:?}",
//         String::from_utf8_lossy(&buf[..50])
//     );

//     // Get memory limit from monitor
//     input_line.clear();
//     monitor_out.write_all("get_memory_limit\n".as_bytes())?;
//     monitor_out.flush()?;
//     monitor_buf_reader.read_line(&mut input_line)?;
//     let memory_limit_mb: u32 = input_line.trim().parse()?;
//     println!("Memory limit is set to {} MB", memory_limit_mb);

//     // akshit added code
//     // let mut executor = Executor::new(disk_in, &mut disk_out, &ctx)?;
//     let mut executor = Executor::new(disk_buf_reader, &mut disk_out, &ctx)?;
//     let result = executor.execute(&query.root)?;

//     // Send result of query to monitor for validation
//     /*
//     monitor_out.write_all("validate\n".as_bytes())?;
//     monitor_out.write_all("1|hello|DBMS|\n".as_bytes())?;
//     monitor_out.write_all("!\n".as_bytes())?;
//     monitor_out.flush()?;
//     */

//     monitor_out.write_all(b"validate\n")?;

//     // for row in result {
//     //     let mut line = String::new();
//     //     for (_, v) in row {
//     //         line.push_str(&v);
//     //         line.push('|');
//     //     }
//     //     line.push('\n');
//     //     monitor_out.write_all(line.as_bytes())?;
//     // }


//     for table_spec in ctx.get_table_specs(){
//         if table_spec.file_id == *FILE_ID.lock().unwrap(){
//             println!("Found matching table spec for file id {}", *FILE_ID.lock().unwrap());
//             for row in &result{
//                 let mut line = String::new();
//                 for column_spec in &table_spec.column_specs {
//                     let column_name = &column_spec.column_name;
//                     let value = row.get(column_name).map(|s| s.as_str()).unwrap_or("");
//                     println!("For column {}, got value {}", column_name, value);
//                     line.push_str(value);
//                     line.push('|');
//                 }
//                 line.push('\n');
//                 monitor_out.write_all(line.as_bytes())?;
//             }
//         }
//     }

//     monitor_out.write_all(b"!\n")?;
//     monitor_out.flush()?;

//     // for row in &result {
//     //     println!("{:?}", row);
//     // }

//     Ok(())
// }

// fn main() -> Result<()> {
//     db_main().with_context(|| "From Database")
// }

use anyhow::{Context, Result};
use clap::Parser;
use common::query::Query;
use db_config::DbContext;
use std::io::{BufRead, BufReader, Write};
use crate::giveoutput::get_output_columns;  // at the top with other use statements

use crate::{
    cli::CliOptions,
    io_setup::{setup_disk_io, setup_monitor_io},
};
use giveoutput::Executor;

mod cli;
mod io_setup;
mod giveoutput;

fn db_main() -> Result<()> {
    let cli_options = CliOptions::parse();

    let ctx = DbContext::load_from_file(cli_options.get_config_path())?;

    for table_spec in ctx.get_table_specs() {
        println!("Table: {}", table_spec.name);
        println!("File id: {}", table_spec.file_id);
        for column_spec in &table_spec.column_specs {
            println!(
                "\tColumn: {} ({:?})",
                column_spec.column_name, column_spec.data_type
            );
        }
        println!();
    }

    let (disk_in, mut disk_out) = setup_disk_io();
    let (monitor_in, mut monitor_out) = setup_monitor_io();

    let mut disk_buf_reader = BufReader::new(disk_in);
    let mut monitor_buf_reader = BufReader::new(monitor_in);

    let mut input_line = String::new();
    monitor_buf_reader.read_line(&mut input_line)?;
    let query: Query = serde_json::from_str(&input_line).unwrap();
    println!("Input query is: {:#?}", query);

    // Get memory limit from monitor
    input_line.clear();
    monitor_out.write_all("get_memory_limit\n".as_bytes())?;
    monitor_out.flush()?;
    monitor_buf_reader.read_line(&mut input_line)?;
    let memory_limit_mb: u32 = input_line.trim().parse()?;
    println!("Memory limit is set to {} MB", memory_limit_mb);

    // NOTE: do NOT issue "get block-size" here manually anymore —
    // Executor::new() does it internally. Any extra disk protocol
    // commands issued here would desync the reader inside Executor.
    let mut executor = Executor::new(disk_buf_reader, &mut disk_out, &ctx)?;
    let result = executor.execute(&query.root)?;

    // Derive column output order directly from the query tree.
    // This correctly handles Scan, Filter, Sort, Project, and Cross.
    let output_columns = get_output_columns(&query.root, &ctx);    println!("Output columns in order: {:?}", output_columns);

    monitor_out.write_all(b"validate\n")?;

    for row in &result {
        let mut line = String::new();
        for col_name in &output_columns {
            // let value = row.get(col_name).map(|s| s.as_str()).unwrap_or("");
            let value = row.iter()
            .find(|(k, _)| k == col_name)
            .map(|(_, v)| v.as_str())
            .unwrap_or("");
            line.push_str(value);
            line.push('|');
        }
        line.push('\n');
        monitor_out.write_all(line.as_bytes())?;
    }

    monitor_out.write_all(b"!\n")?;
    monitor_out.flush()?;

    Ok(())
}

fn main() -> Result<()> {
    db_main().with_context(|| "From Monitor")
}