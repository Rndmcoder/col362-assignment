// main.rs
use anyhow::{Context, Result};
use clap::Parser;
use common::query::Query;
use db_config::DbContext;
use std::io::{BufRead, BufReader, Write};

use crate::giveoutput::{get_output_columns, RunSet};
use crate::{cli::CliOptions, io_setup::{setup_disk_io, setup_monitor_io}};
use giveoutput::Executor;
use query_optimiser::{optimise_with_ctx, OptContext};

mod cli;
mod io_setup;
mod giveoutput;
mod query_optimiser;

fn db_main() -> Result<()> {
    let cli_options = CliOptions::parse();

    let ctx = DbContext::load_from_file(cli_options.get_config_path())
        .context("Cannot load db_config.json — run the generator first")?;

    eprintln!("[MAIN] loaded {} tables", ctx.get_table_specs().len());
    for table in ctx.get_table_specs() {
        eprintln!("[MAIN] TABLE '{}' ({} cols)", table.name, table.column_specs.len());
        for col in &table.column_specs {
            eprintln!("  COL {} {:?}", col.column_name, col.data_type);
        }
    }

    let (disk_in, mut disk_out)    = setup_disk_io();
    let (monitor_in, mut monitor_out) = setup_monitor_io();
    let mut disk_buf = BufReader::new(disk_in);
    let mut mon_buf  = BufReader::new(monitor_in);

    let mut line = String::new();
    mon_buf.read_line(&mut line)?;
    let query: Query = serde_json::from_str(&line)
        .map_err(|e| anyhow::anyhow!("query parse: {}\nraw: {}", e, line.trim()))?;
    eprintln!("[MAIN] query={:#?}", query);

    line.clear();
    monitor_out.write_all(b"get_memory_limit\n")?;
    monitor_out.flush()?;
    mon_buf.read_line(&mut line)?;
    let memory_limit_mb: u32 = line.trim().parse()
        .map_err(|e| anyhow::anyhow!("memory_limit: '{}': {}", line.trim(), e))?;
    eprintln!("[MAIN] memory_limit_mb={}", memory_limit_mb);

    let mut opt_ctx = OptContext::new(&ctx);
    opt_ctx.memory_budget_bytes = (memory_limit_mb as usize) * 1024 * 1024;

    let opt_plan = optimise_with_ctx(query.root, &opt_ctx);
    eprintln!("[MAIN] plan={:#?}", opt_plan);

    let mut executor = Executor::new(disk_buf, &mut disk_out, &ctx)?;
    executor.set_memory_limit_mb(memory_limit_mb);

    let result = executor.execute(&opt_plan)?;
    eprintln!("[MAIN] result rows={}", result.row_count());

    let output_columns = get_output_columns(&opt_plan, &ctx);
    eprintln!("[MAIN] output_columns={:?}", output_columns);

    monitor_out.write_all(b"validate\n")?;

    let bs = executor.block_size as usize;
    match result {
        RunSet::InMemory(rows) => {
            for (i, row) in rows.iter().enumerate() {
                write_row(&mut monitor_out, &output_columns, row, i)?;
            }
        }
        RunSet::Spilled { runs, .. } => {
            let mut row_idx = 0usize;
            for run in &runs {
                for bi in 0..run.num_blocks {
                    let buf = executor.read_block_pub(run.start_block + bi)?;
                    let b   = &buf[..];
                    let rc  = u16::from_le_bytes([b[bs-2], b[bs-1]]) as usize;
                    let mut off = 0usize;
                    for _ in 0..rc {
                        if let Some(row) = giveoutput::decode_row_pub(b, &mut off) {
                            write_row(&mut monitor_out, &output_columns, &row, row_idx)?;
                            row_idx += 1;
                        }
                    }
                }
            }
        }
    }

    monitor_out.write_all(b"!\n")?;
    monitor_out.flush()?;
    eprintln!("[MAIN] done");
    Ok(())
}

fn write_row<W: Write>(out: &mut W, cols: &[String], row: &giveoutput::Row, idx: usize) -> Result<()> {
    let mut line = String::new();
    for col in cols {
        let v = row.iter().find(|(k, _)| k == col).map(|(_, v)| v.as_str())
            .unwrap_or_else(|| { eprintln!("[MAIN] row {} missing col '{}'", idx, col); "" });
        line.push_str(v);
        line.push('|');
    }
    line.push('\n');
    out.write_all(line.as_bytes())?;
    Ok(())
}

fn main() -> Result<()> {
    db_main().with_context(|| "database error")
}
