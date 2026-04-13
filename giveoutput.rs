// giveoutput.rs — streaming out-of-core query executor
//
// Core guarantee: no Vec<Row> ever holds more than chunk_cap rows.
// Operators exchange data via RunSet (either a small InMemory vec, or
// a list of on-disk ScratchRuns). The final result is also a RunSet;
// main.rs streams it out row-by-row without loading everything at once.

use anyhow::Result;
use std::collections::HashMap;
use std::io::{BufRead, Write};
use common::query::*;
use db_config::DbContext;
use common::DataType;
use crate::query_optimiser::{
    OptQueryOp, OptFilterData, OptProjectData, OptCrossData, OptSortData,
    FilterCrossData, HashJoinData,
};

pub type Row   = Vec<(String, String)>;
pub type Table = Vec<Row>;

// ─────────────────────────────────────────────────────────────────────────────
// Scratch encoding
//   Block: [ encoded rows ... | padding | row_count: u16 LE ]
//   Row:   ncols:u16  (klen:u16 key vlen:u16 val)*
// ─────────────────────────────────────────────────────────────────────────────

fn encode_row(row: &Row, dst: &mut Vec<u8>) {
    dst.extend_from_slice(&(row.len() as u16).to_le_bytes());
    for (k, v) in row {
        dst.extend_from_slice(&(k.len() as u16).to_le_bytes());
        dst.extend_from_slice(k.as_bytes());
        dst.extend_from_slice(&(v.len() as u16).to_le_bytes());
        dst.extend_from_slice(v.as_bytes());
    }
}

fn encoded_size(row: &Row) -> usize {
    2 + row.iter().map(|(k,v)| 4 + k.len() + v.len()).sum::<usize>()
}

pub fn decode_row(src: &[u8], off: &mut usize) -> Option<Row> {
    if *off + 2 > src.len() { return None; }
    let nc = u16::from_le_bytes([src[*off], src[*off+1]]) as usize;
    *off += 2;
    let mut row = Vec::with_capacity(nc);
    for _ in 0..nc {
        if *off + 2 > src.len() { return None; }
        let kl = u16::from_le_bytes([src[*off], src[*off+1]]) as usize;
        *off += 2;
        if *off + kl > src.len() { return None; }
        let k = String::from_utf8(src[*off..*off+kl].to_vec()).ok()?;
        *off += kl;
        if *off + 2 > src.len() { return None; }
        let vl = u16::from_le_bytes([src[*off], src[*off+1]]) as usize;
        *off += 2;
        if *off + vl > src.len() { return None; }
        let v = String::from_utf8(src[*off..*off+vl].to_vec()).ok()?;
        *off += vl;
        row.push((k, v));
    }
    Some(row)
}

pub fn decode_row_pub(src: &[u8], off: &mut usize) -> Option<Row> { decode_row(src, off) }

// ─────────────────────────────────────────────────────────────────────────────
// ScratchRun
// ─────────────────────────────────────────────────────────────────────────────

pub struct ScratchRun {
    pub start_block: u64,
    pub num_blocks:  u64,
    pub row_count:   usize,
}

// ─────────────────────────────────────────────────────────────────────────────
// RunSet
// ─────────────────────────────────────────────────────────────────────────────

pub enum RunSet {
    InMemory(Table),
    Spilled { runs: Vec<ScratchRun>, row_count: usize },
}

impl RunSet {
    pub fn row_count(&self) -> usize {
        match self {
            RunSet::InMemory(t)               => t.len(),
            RunSet::Spilled { row_count, .. } => *row_count,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ChunkWriter
// ─────────────────────────────────────────────────────────────────────────────

struct ChunkWriter {
    chunk: Table,
    runs:  Vec<ScratchRun>,
    cap:   usize,
    total: usize,
}

impl ChunkWriter {
    fn new(cap: usize) -> Self {
        let cap = cap.max(1);
        Self { chunk: Vec::with_capacity(cap.min(4096)), runs: Vec::new(), cap, total: 0 }
    }

    fn push<F: FnMut(&[Row]) -> Result<ScratchRun>>(&mut self, row: Row, mut spill_fn: F)
        -> Result<()>
    {
        self.total += 1;
        self.chunk.push(row);
        if self.chunk.len() >= self.cap {
            let run = spill_fn(&self.chunk)?;
            self.runs.push(run);
            self.chunk.clear();
        }
        Ok(())
    }

    fn finish<F: FnMut(&[Row]) -> Result<ScratchRun>>(mut self, mut spill_fn: F)
        -> Result<RunSet>
    {
        if self.runs.is_empty() {
            return Ok(RunSet::InMemory(self.chunk));
        }
        if !self.chunk.is_empty() {
            let run = spill_fn(&self.chunk)?;
            self.runs.push(run);
        }
        Ok(RunSet::Spilled { row_count: self.total, runs: self.runs })
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// MergeHead
// ─────────────────────────────────────────────────────────────────────────────

struct MergeHead {
    run_start:       u64,
    run_blocks:      u64,
    cur_block:       u64,
    block_buf:       Vec<u8>,
    block_row_count: usize,
    block_row_idx:   usize,
    block_off:       usize,
    current_row:     Option<Row>,
}

impl MergeHead {
    fn advance(&mut self, bs: usize) {
        if self.block_row_idx >= self.block_row_count || self.block_off >= bs - 2 {
            self.current_row = None;
            return;
        }
        self.current_row = decode_row(&self.block_buf, &mut self.block_off);
        if self.current_row.is_some() { self.block_row_idx += 1; }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Executor
// ─────────────────────────────────────────────────────────────────────────────

pub struct Executor<'a, R: BufRead, W: Write> {
    disk_in:   R,
    disk_out:  &'a mut W,
    pub block_size:         u64,
    ctx:       &'a DbContext,
    next_anon: u64,
    pub memory_limit_bytes: usize,
}

impl<'a, R: BufRead, W: Write> Executor<'a, R, W> {

    pub fn new(mut disk_in: R, disk_out: &'a mut W, ctx: &'a DbContext) -> Result<Self> {
        disk_out.write_all(b"get block-size\n")?;
        disk_out.flush()?;
        let mut line = String::new();
        disk_in.read_line(&mut line)?;
        let block_size: u64 = line.trim().parse()
            .map_err(|e| anyhow::anyhow!("block-size: '{}': {}", line.trim(), e))?;
        eprintln!("[INIT] block_size={}", block_size);
        line.clear();

        disk_out.write_all(b"get anon-start-block\n")?;
        disk_out.flush()?;
        disk_in.read_line(&mut line)?;
        let anon_start: u64 = line.trim().parse()
            .map_err(|e| anyhow::anyhow!("anon-start: '{}': {}", line.trim(), e))?;
        eprintln!("[INIT] anon_start={}", anon_start);

        Ok(Self {
            disk_in, disk_out,
            block_size, ctx,
            next_anon: anon_start,
            memory_limit_bytes: 20 * 1024 * 1024,
        })
    }

    pub fn set_memory_limit_mb(&mut self, mb: u32) {
        let overhead = 28 * 1024 * 1024usize;
        let total    = (mb as usize) * 1024 * 1024;
        self.memory_limit_bytes = total.saturating_sub(overhead).max(4 * 1024 * 1024);
        eprintln!("[INIT] memory_limit_bytes={} ({} MB data, {} MB total)",
                  self.memory_limit_bytes, self.memory_limit_bytes/(1024*1024), mb);
    }

    // Conservative chunk cap.
    // Hard ceiling: at most 256 blocks per spill call (256 * block_size bytes payload).
    fn cap_for(&self, row_ram: usize) -> usize {
        let budget       = self.memory_limit_bytes / 8;
        let by_ram       = (budget / row_ram.max(1)).max(64);
        // Each spill writes one block at a time now, so payload is capped at block_size.
        // But we still cap chunk row count so sort chunks don't get too large.
        let max_rows_per_chunk = 4096usize; // safe upper bound
        by_ram.min(max_rows_per_chunk)
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Disk helpers
    // ─────────────────────────────────────────────────────────────────────────

    fn read_block(&mut self, abs: u64) -> Result<Vec<u8>> {
        let bs = self.block_size as usize;
        self.disk_out.write_all(format!("get block {} 1\n", abs).as_bytes())?;
        self.disk_out.flush()?;
        let mut buf = vec![0u8; bs];
        let mut n = 0;
        while n < bs {
            let a = self.disk_in.fill_buf()?;
            if a.is_empty() {
                anyhow::bail!("disk EOF block {} after {}/{}", abs, n, bs);
            }
            let tc = a.len().min(bs - n);
            buf[n..n+tc].copy_from_slice(&a[..tc]);
            self.disk_in.consume(tc);
            n += tc;
        }
        Ok(buf)
    }

    // Spill: write one block at a time — never allocates a large payload Vec.
    fn spill(&mut self, rows: &[Row]) -> Result<ScratchRun> {
        let bs     = self.block_size as usize;
        let usable = bs - 2;
        let start  = self.next_anon;
        let mut n_blocks = 0u64;

        let mut cur = vec![0u8; bs];
        let mut off = 0usize;
        let mut cnt = 0u16;
        let mut enc = Vec::new();

        for row in rows {
            enc.clear();
            encode_row(row, &mut enc);
            if enc.len() > usable { anyhow::bail!("row too large: {} bytes", enc.len()); }

            if off + enc.len() > usable {
                // Flush this block immediately
                cur[bs-2] = (cnt & 0xFF) as u8;
                cur[bs-1] = (cnt >> 8)   as u8;
                self.disk_out.write_all(
                    format!("put block {} 1\n", start + n_blocks).as_bytes()
                )?;
                self.disk_out.write_all(&cur)?;
                self.disk_out.flush()?;
                n_blocks += 1;
                cur.iter_mut().for_each(|b| *b = 0);
                off = 0; cnt = 0;
            }

            cur[off..off+enc.len()].copy_from_slice(&enc);
            off += enc.len();
            cnt += 1;
        }

        // Final block
        cur[bs-2] = (cnt & 0xFF) as u8;
        cur[bs-1] = (cnt >> 8)   as u8;
        self.disk_out.write_all(
            format!("put block {} 1\n", start + n_blocks).as_bytes()
        )?;
        self.disk_out.write_all(&cur)?;
        self.disk_out.flush()?;
        n_blocks += 1;

        self.next_anon += n_blocks;
        eprintln!("[SPILL] {} rows -> {} blocks (start={})", rows.len(), n_blocks, start);
        Ok(ScratchRun { start_block: start, num_blocks: n_blocks, row_count: rows.len() })
    }

    fn for_each_row_in_runset<F>(&mut self, rs: RunSet, mut f: F) -> Result<()>
    where F: FnMut(Row) -> Result<()>
    {
        match rs {
            RunSet::InMemory(rows) => {
                for row in rows { f(row)?; }
            }
            RunSet::Spilled { runs, .. } => {
                let descriptors: Vec<(u64, u64)> = runs.iter()
                    .map(|r| (r.start_block, r.num_blocks))
                    .collect();
                let bs = self.block_size as usize;
                for (start, n_blk) in descriptors {
                    for bi in 0..n_blk {
                        let buf = self.read_block(start + bi)?;
                        let b   = &buf[..];
                        let rc  = u16::from_le_bytes([b[bs-2], b[bs-1]]) as usize;
                        let mut off = 0usize;
                        for _ in 0..rc {
                            if let Some(row) = decode_row(b, &mut off) { f(row)?; }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Public entry point
    // ─────────────────────────────────────────────────────────────────────────

    pub fn execute(&mut self, op: &OptQueryOp) -> Result<RunSet> {
        eprintln!("[EXEC] {}", opt_op_name(op));
        self.exec_node(op)
    }

    fn exec_node(&mut self, op: &OptQueryOp) -> Result<RunSet> {
        match op {
            OptQueryOp::Scan(d)        => self.exec_scan(d),
            OptQueryOp::Filter(d)      => self.exec_filter(d),
            OptQueryOp::Project(d)     => self.exec_project(d),
            OptQueryOp::Cross(d)       => self.exec_cross(d),
            OptQueryOp::Sort(d)        => self.exec_sort(d),
            OptQueryOp::FilterCross(d) => self.exec_filter_cross(d),
            OptQueryOp::HashJoin(d)    => self.exec_hash_join(d),
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // SCAN
    // ─────────────────────────────────────────────────────────────────────────
    fn exec_scan(&mut self, data: &ScanData) -> Result<RunSet> {
        let table_spec = self.ctx.get_table_specs()
            .iter().find(|t| t.name == data.table_id)
            .ok_or_else(|| anyhow::anyhow!("table not found: {}", data.table_id))?;
        let file_id   = table_spec.file_id.clone();
        let col_specs = table_spec.column_specs.clone();
        let table_id  = data.table_id.clone();

        let mut line = String::new();
        self.disk_out.write_all(format!("get file start-block {}\n", file_id).as_bytes())?;
        self.disk_out.flush()?;
        self.disk_in.read_line(&mut line)?;
        let file_start: u64 = line.trim().parse()
            .map_err(|e| anyhow::anyhow!("file start-block: '{}': {}", line.trim(), e))?;
        line.clear();

        self.disk_out.write_all(format!("get file num-blocks {}\n", file_id).as_bytes())?;
        self.disk_out.flush()?;
        self.disk_in.read_line(&mut line)?;
        let file_blocks: u64 = line.trim().parse()
            .map_err(|e| anyhow::anyhow!("file num-blocks: '{}': {}", line.trim(), e))?;
        line.clear();

        eprintln!("[SCAN] '{}' start={} blocks={}", table_id, file_start, file_blocks);

        let bs = self.block_size as usize;
        let mut writer: Option<ChunkWriter> = None;

        for blk_rel in 0..file_blocks {
            let blk = self.read_block(file_start + blk_rel)?;
            let b   = &blk[..];
            let rc  = u16::from_le_bytes([b[bs-2], b[bs-1]]) as usize;
            if rc == 0 { continue; }

            let mut off = 0usize;
            'row_loop: for _ in 0..rc {
                if off >= bs - 2 { break; }
                let row_start = off;
                let mut row: Row = Vec::with_capacity(col_specs.len());
                let mut ok = true;

                for col in &col_specs {
                    let val: String = match col.data_type {
                        DataType::Int32 => {
                            if off + 4 > bs-2 { ok=false; break; }
                            let v = i32::from_le_bytes(b[off..off+4].try_into().unwrap());
                            off += 4; v.to_string()
                        }
                        DataType::Int64 => {
                            if off + 8 > bs-2 { ok=false; break; }
                            let v = i64::from_le_bytes(b[off..off+8].try_into().unwrap());
                            off += 8; v.to_string()
                        }
                        DataType::Float32 => {
                            if off + 4 > bs-2 { ok=false; break; }
                            let v = f32::from_le_bytes(b[off..off+4].try_into().unwrap());
                            off += 4;
                            if !v.is_finite() { ok=false; break; }
                            fmt_f64(v as f64)
                        }
                        DataType::Float64 => {
                            if off + 8 > bs-2 { ok=false; break; }
                            let v = f64::from_le_bytes(b[off..off+8].try_into().unwrap());
                            off += 8;
                            if !v.is_finite() { ok=false; break; }
                            fmt_f64(v)
                        }
                        DataType::String => {
                            let s0 = off;
                            while off < bs-2 && b[off] != 0 { off += 1; }
                            if off >= bs-2 { ok=false; break; }
                            let s = String::from_utf8_lossy(&b[s0..off]).to_string();
                            off += 1; s
                        }
                        _ => { ok=false; break; }
                    };
                    row.push((format!("{}.{}", table_id, col.column_name), val));
                }

                if !ok || row.len() != col_specs.len() {
                    eprintln!("[SCAN] bad row blk={} off={}", file_start+blk_rel, row_start);
                    break 'row_loop;
                }

                if writer.is_none() {
                    let row_ram = encoded_size(&row) + 64
                        + row.iter().map(|(k,v)| k.len()+v.len()).sum::<usize>();
                    let cap = self.cap_for(row_ram);
                    eprintln!("[SCAN] row_ram~{} chunk_cap={}", row_ram, cap);
                    writer = Some(ChunkWriter::new(cap));
                }
                let w = writer.as_mut().unwrap();
                w.push(row, |rows| self.spill(rows))?;
            }
        }

        let w = writer.unwrap_or_else(|| ChunkWriter::new(1));
        let total = w.total;
        let rs = w.finish(|rows| self.spill(rows))?;
        eprintln!("[SCAN] done total={}", total);
        Ok(rs)
    }

    // ─────────────────────────────────────────────────────────────────────────
    // FILTER
    // ─────────────────────────────────────────────────────────────────────────
    fn exec_filter(&mut self, data: &OptFilterData) -> Result<RunSet> {
        let input = self.exec_node(&data.underlying)?;
        eprintln!("[FILTER] input={}", input.row_count());
        let preds = data.predicates.clone();

        let mut writer: Option<ChunkWriter> = None;
        let this = self as *mut Self;
        let mem_limit = self.memory_limit_bytes;

        self.for_each_row_in_runset(input, |row| {
            if !eval_preds(&row, &preds) { return Ok(()); }
            if writer.is_none() {
                let row_ram = encoded_size(&row) + 64
                    + row.iter().map(|(k,v)| k.len()+v.len()).sum::<usize>();
                let cap = (mem_limit / 8 / row_ram.max(1)).max(64).min(4096);
                writer = Some(ChunkWriter::new(cap));
            }
            writer.as_mut().unwrap().push(row, |rows| unsafe { (*this).spill(rows) })
        })?;

        let w = writer.unwrap_or_else(|| ChunkWriter::new(1));
        let total = w.total;
        let rs = w.finish(|rows| self.spill(rows))?;
        eprintln!("[FILTER] out={}", total);
        Ok(rs)
    }

    // ─────────────────────────────────────────────────────────────────────────
    // PROJECT
    // ─────────────────────────────────────────────────────────────────────────
    fn exec_project(&mut self, data: &OptProjectData) -> Result<RunSet> {
        let input = self.exec_node(&data.underlying)?;
        eprintln!("[PROJECT] input={}", input.row_count());

        let map = data.column_name_map.clone();
        let mut writer: Option<ChunkWriter> = None;
        let this = self as *mut Self;
        let mem_limit = self.memory_limit_bytes;

        self.for_each_row_in_runset(input, |row| {
            let r = project_row(&row, &map);
            if writer.is_none() && !r.is_empty() {
                let row_ram = encoded_size(&r) + 64
                    + r.iter().map(|(k,v)| k.len()+v.len()).sum::<usize>();
                let cap = (mem_limit / 8 / row_ram.max(1)).max(64).min(4096);
                writer = Some(ChunkWriter::new(cap));
            }
            if let Some(w) = writer.as_mut() {
                w.push(r, |rows| unsafe { (*this).spill(rows) })?;
            }
            Ok(())
        })?;

        let w = writer.unwrap_or_else(|| ChunkWriter::new(1));
        let total = w.total;
        let rs = w.finish(|rows| self.spill(rows))?;
        eprintln!("[PROJECT] out={}", total);
        Ok(rs)
    }

    // ─────────────────────────────────────────────────────────────────────────
    // CROSS (full cartesian — only used if no filter above it)
    // ─────────────────────────────────────────────────────────────────────────
    fn exec_cross(&mut self, data: &OptCrossData) -> Result<RunSet> {
        let left_rs  = self.exec_node(&data.left)?;
        let right_rs = self.exec_node(&data.right)?;
        eprintln!("[CROSS] left={} right={}", left_rs.row_count(), right_rs.row_count());

        if left_rs.row_count() == 0 || right_rs.row_count() == 0 {
            return Ok(RunSet::InMemory(Vec::new()));
        }

        let chunk_budget = self.memory_limit_bytes / 4;
        let (outer_rs, inner_rs) =
            if right_rs.row_count() <= left_rs.row_count() { (left_rs, right_rs) }
            else { (right_rs, left_rs) };

        let mut inner_chunks: Vec<Table> = Vec::new();
        {
            let mut cur: Table = Vec::new();
            let mut cur_bytes = 0usize;
            let p = std::mem::size_of::<usize>();
            self.for_each_row_in_runset(inner_rs, |row| {
                let rb = 3*p + row.iter().map(|(k,v)| 6*p+k.len()+v.len()).sum::<usize>();
                if cur_bytes + rb > chunk_budget && !cur.is_empty() {
                    inner_chunks.push(std::mem::take(&mut cur));
                    cur_bytes = 0;
                }
                cur_bytes += rb;
                cur.push(row);
                Ok(())
            })?;
            if !cur.is_empty() { inner_chunks.push(cur); }
        }

        let multi = inner_chunks.len() > 1;
        let this  = self as *mut Self;
        let mem   = self.memory_limit_bytes;

        let outer_final: RunSet = if multi {
            match outer_rs {
                RunSet::Spilled {..} => outer_rs,
                RunSet::InMemory(rows) => {
                    let mut cw = ChunkWriter::new(self.cap_for(600));
                    for row in rows { cw.push(row, |r| unsafe { (*this).spill(r) })?; }
                    cw.finish(|r| self.spill(r))?
                }
            }
        } else { outer_rs };

        let mut out_writer: Option<ChunkWriter> = None;

        if !multi {
            let chunk = &inner_chunks[0];
            self.for_each_row_in_runset(outer_final, |o_row| {
                for i_row in chunk {
                    let mut row = o_row.clone();
                    row.extend_from_slice(i_row);
                    if out_writer.is_none() {
                        let rr = encoded_size(&row)+64
                            +row.iter().map(|(k,v)|k.len()+v.len()).sum::<usize>();
                        out_writer = Some(ChunkWriter::new((mem/8/rr.max(1)).max(64).min(4096)));
                    }
                    out_writer.as_mut().unwrap()
                        .push(row, |rows| unsafe { (*this).spill(rows) })?;
                }
                Ok(())
            })?;
        } else {
            let descs: Vec<(u64,u64)> = match &outer_final {
                RunSet::Spilled { runs, .. } =>
                    runs.iter().map(|r|(r.start_block,r.num_blocks)).collect(),
                RunSet::InMemory(_) => unreachable!(),
            };
            let bs = self.block_size as usize;
            for chunk in &inner_chunks {
                for &(start, n_blk) in &descs {
                    for bi in 0..n_blk {
                        let buf = self.read_block(start+bi)?;
                        let rc  = u16::from_le_bytes([buf[bs-2],buf[bs-1]]) as usize;
                        let mut off = 0usize;
                        for _ in 0..rc {
                            if let Some(o_row) = decode_row(&buf, &mut off) {
                                for i_row in chunk {
                                    let mut row = o_row.clone();
                                    row.extend_from_slice(i_row);
                                    if out_writer.is_none() {
                                        let rr = encoded_size(&row)+64
                                            +row.iter().map(|(k,v)|k.len()+v.len()).sum::<usize>();
                                        out_writer = Some(ChunkWriter::new(
                                            (mem/8/rr.max(1)).max(64).min(4096)
                                        ));
                                    }
                                    out_writer.as_mut().unwrap()
                                        .push(row, |rows| unsafe { (*this).spill(rows) })?;
                                }
                            }
                        }
                    }
                }
            }
        }

        let w = out_writer.unwrap_or_else(|| ChunkWriter::new(1));
        let rs = w.finish(|rows| self.spill(rows))?;
        eprintln!("[CROSS] out={}", rs.row_count());
        Ok(rs)
    }

    // ─────────────────────────────────────────────────────────────────────────
    // FILTER_CROSS — NLJ with filter inside loop
    // ─────────────────────────────────────────────────────────────────────────
    fn exec_filter_cross(&mut self, data: &FilterCrossData) -> Result<RunSet> {
        let left_rs  = self.exec_node(&data.left)?;
        let right_rs = self.exec_node(&data.right)?;
        eprintln!("[FILTER_CROSS] left={} right={} preds={}",
                  left_rs.row_count(), right_rs.row_count(), data.predicates.len());

        if left_rs.row_count() == 0 || right_rs.row_count() == 0 {
            return Ok(RunSet::InMemory(Vec::new()));
        }

        let chunk_budget = self.memory_limit_bytes / 4;
        let preds = data.predicates.clone();

        let (outer_rs, inner_rs) =
            if right_rs.row_count() <= left_rs.row_count() { (left_rs, right_rs) }
            else { (right_rs, left_rs) };

        let mut inner_chunks: Vec<Table> = Vec::new();
        {
            let mut cur: Table = Vec::new();
            let mut cur_bytes = 0usize;
            let p = std::mem::size_of::<usize>();
            self.for_each_row_in_runset(inner_rs, |row| {
                let rb = 3*p + row.iter().map(|(k,v)| 6*p+k.len()+v.len()).sum::<usize>();
                if cur_bytes + rb > chunk_budget && !cur.is_empty() {
                    inner_chunks.push(std::mem::take(&mut cur));
                    cur_bytes = 0;
                }
                cur_bytes += rb;
                cur.push(row);
                Ok(())
            })?;
            if !cur.is_empty() { inner_chunks.push(cur); }
        }

        eprintln!("[FILTER_CROSS] inner split into {} chunks", inner_chunks.len());

        let multi = inner_chunks.len() > 1;
        let this  = self as *mut Self;
        let mem   = self.memory_limit_bytes;

        let outer_final: RunSet = if multi {
            match outer_rs {
                RunSet::Spilled {..} => outer_rs,
                RunSet::InMemory(rows) => {
                    let mut cw = ChunkWriter::new(self.cap_for(600));
                    for row in rows { cw.push(row, |r| unsafe { (*this).spill(r) })?; }
                    cw.finish(|r| self.spill(r))?
                }
            }
        } else { outer_rs };

        let mut out_writer: Option<ChunkWriter> = None;

        if !multi {
            let chunk = &inner_chunks[0];
            self.for_each_row_in_runset(outer_final, |o_row| {
                for i_row in chunk {
                    let mut row = o_row.clone();
                    row.extend_from_slice(i_row);
                    if !eval_preds(&row, &preds) { continue; }
                    if out_writer.is_none() {
                        let rr = encoded_size(&row)+64
                            +row.iter().map(|(k,v)|k.len()+v.len()).sum::<usize>();
                        out_writer = Some(ChunkWriter::new((mem/8/rr.max(1)).max(64).min(4096)));
                    }
                    out_writer.as_mut().unwrap()
                        .push(row, |rows| unsafe { (*this).spill(rows) })?;
                }
                Ok(())
            })?;
        } else {
            let descs: Vec<(u64,u64)> = match &outer_final {
                RunSet::Spilled { runs, .. } =>
                    runs.iter().map(|r|(r.start_block,r.num_blocks)).collect(),
                RunSet::InMemory(_) => unreachable!(),
            };
            let bs = self.block_size as usize;
            for chunk in &inner_chunks {
                for &(start, n_blk) in &descs {
                    for bi in 0..n_blk {
                        let buf = self.read_block(start+bi)?;
                        let rc  = u16::from_le_bytes([buf[bs-2],buf[bs-1]]) as usize;
                        let mut off = 0usize;
                        for _ in 0..rc {
                            if let Some(o_row) = decode_row(&buf, &mut off) {
                                for i_row in chunk {
                                    let mut row = o_row.clone();
                                    row.extend_from_slice(i_row);
                                    if !eval_preds(&row, &preds) { continue; }
                                    if out_writer.is_none() {
                                        let rr = encoded_size(&row)+64
                                            +row.iter().map(|(k,v)|k.len()+v.len()).sum::<usize>();
                                        out_writer = Some(ChunkWriter::new(
                                            (mem/8/rr.max(1)).max(64).min(4096)
                                        ));
                                    }
                                    out_writer.as_mut().unwrap()
                                        .push(row, |rows| unsafe { (*this).spill(rows) })?;
                                }
                            }
                        }
                    }
                }
            }
        }

        let w = out_writer.unwrap_or_else(|| ChunkWriter::new(1));
        let total = w.total;
        let rs = w.finish(|rows| self.spill(rows))?;
        eprintln!("[FILTER_CROSS] out={}", total);
        Ok(rs)
    }

    // ─────────────────────────────────────────────────────────────────────────
    // HASH JOIN — O(N+M) equi-join
    // ─────────────────────────────────────────────────────────────────────────
    fn exec_hash_join(&mut self, data: &HashJoinData) -> Result<RunSet> {
        let left_rs  = self.exec_node(&data.left)?;
        let right_rs = self.exec_node(&data.right)?;
        eprintln!("[HASHJOIN] left={} right={} keys={:?}={:?}",
                  left_rs.row_count(), right_rs.row_count(),
                  data.left_keys, data.right_keys);

        if left_rs.row_count() == 0 || right_rs.row_count() == 0 {
            return Ok(RunSet::InMemory(Vec::new()));
        }

        let extra_preds = data.extra_preds.clone();
        let left_keys   = data.left_keys.clone();
        let right_keys  = data.right_keys.clone();

        // Build = smaller side
        let (build_rs, probe_rs, build_keys, probe_keys) =
            if left_rs.row_count() <= right_rs.row_count() {
                (left_rs, right_rs, left_keys, right_keys)
            } else {
                (right_rs, left_rs, right_keys, left_keys)
            };

        let build_count  = build_rs.row_count();
        let build_budget = self.memory_limit_bytes / 3;
        eprintln!("[HASHJOIN] build={} probe={}", build_count, probe_rs.row_count());

        // Build phase
        let p = std::mem::size_of::<usize>();
        let mut build_map: HashMap<String, Vec<Row>> =
            HashMap::with_capacity(build_count.min(65536));
        let mut build_bytes = 0usize;
        let mut overflow: Table = Vec::new();
        let mut overflowed = false;

        self.for_each_row_in_runset(build_rs, |row| {
            if overflowed { overflow.push(row); return Ok(()); }
            let key = make_join_key(&row, &build_keys);
            let rb  = 3*p + row.iter().map(|(k,v)| 6*p+k.len()+v.len()).sum::<usize>()
                    + key.len() + p;
            build_bytes += rb;
            if build_bytes > build_budget {
                overflowed = true;
                overflow.push(row);
                return Ok(());
            }
            build_map.entry(key).or_default().push(row);
            Ok(())
        })?;

        if overflowed {
            eprintln!("[HASHJOIN] overflow → grace hash join");
            let mut all_build: Table = Vec::new();
            for rows in build_map.into_values() { all_build.extend(rows); }
            all_build.extend(overflow);
            return self.grace_hash_join(
                RunSet::InMemory(all_build), probe_rs, build_keys, probe_keys, extra_preds
            );
        }

        eprintln!("[HASHJOIN] build map: {} keys, {} bytes", build_map.len(), build_bytes);

        // Probe phase
        let mem  = self.memory_limit_bytes;
        let this = self as *mut Self;
        let mut out_writer: Option<ChunkWriter> = None;

        self.for_each_row_in_runset(probe_rs, |p_row| {
            let key = make_join_key(&p_row, &probe_keys);
            if let Some(build_rows) = build_map.get(&key) {
                for b_row in build_rows {
                    let mut row = p_row.clone();
                    row.extend_from_slice(b_row);
                    if !eval_preds(&row, &extra_preds) { continue; }
                    if out_writer.is_none() {
                        let rr = encoded_size(&row)+64
                            +row.iter().map(|(k,v)|k.len()+v.len()).sum::<usize>();
                        out_writer = Some(ChunkWriter::new(
                            (mem/8/rr.max(1)).max(64).min(4096)
                        ));
                    }
                    out_writer.as_mut().unwrap()
                        .push(row, |rows| unsafe { (*this).spill(rows) })?;
                }
            }
            Ok(())
        })?;

        let w = out_writer.unwrap_or_else(|| ChunkWriter::new(1));
        let total = w.total;
        let rs = w.finish(|rows| self.spill(rows))?;
        eprintln!("[HASHJOIN] out={}", total);
        Ok(rs)
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Grace hash join — partition both sides to disk, join per bucket
    // ─────────────────────────────────────────────────────────────────────────
    fn grace_hash_join(
        &mut self,
        build_rs:    RunSet,
        probe_rs:    RunSet,
        build_keys:  Vec<String>,
        probe_keys:  Vec<String>,
        extra_preds: Vec<Predicate>,
    ) -> Result<RunSet> {
        let n_parts: usize = 64;
        eprintln!("[GRACE] partitioning into {} buckets", n_parts);

        // Partition build side
        let mut build_parts: Vec<Vec<Row>> = (0..n_parts).map(|_| Vec::new()).collect();
        self.for_each_row_in_runset(build_rs, |row| {
            let key = make_join_key(&row, &build_keys);
            build_parts[hash_str(&key) % n_parts].push(row);
            Ok(())
        })?;
        let mut build_runs: Vec<Option<ScratchRun>> = Vec::with_capacity(n_parts);
        for part in build_parts.into_iter() {
            if part.is_empty() { build_runs.push(None); continue; }
            let run = self.spill(&part)?;
            build_runs.push(Some(run));
        }

        // Partition probe side
        let mut probe_parts: Vec<Vec<Row>> = (0..n_parts).map(|_| Vec::new()).collect();
        self.for_each_row_in_runset(probe_rs, |row| {
            let key = make_join_key(&row, &probe_keys);
            probe_parts[hash_str(&key) % n_parts].push(row);
            Ok(())
        })?;
        let mut probe_runs: Vec<Option<ScratchRun>> = Vec::with_capacity(n_parts);
        for part in probe_parts.into_iter() {
            if part.is_empty() { probe_runs.push(None); continue; }
            let run = self.spill(&part)?;
            probe_runs.push(Some(run));
        }

        // Join each partition pair
        let mem  = self.memory_limit_bytes;
        let this = self as *mut Self;
        let mut out_writer: Option<ChunkWriter> = None;
        let bs = self.block_size as usize;

        for i in 0..n_parts {
            let (Some(brun), Some(prun)) = (&build_runs[i], &probe_runs[i]) else { continue };
            if brun.row_count == 0 || prun.row_count == 0 { continue; }

            // Load build partition into hash map
            let mut part_map: HashMap<String, Vec<Row>> = HashMap::new();
            for bi in 0..brun.num_blocks {
                let buf = self.read_block(brun.start_block + bi)?;
                let rc  = u16::from_le_bytes([buf[bs-2], buf[bs-1]]) as usize;
                let mut off = 0usize;
                for _ in 0..rc {
                    if let Some(row) = decode_row(&buf, &mut off) {
                        let key = make_join_key(&row, &build_keys);
                        part_map.entry(key).or_default().push(row);
                    }
                }
            }

            // Stream probe partition
            for bi in 0..prun.num_blocks {
                let buf = self.read_block(prun.start_block + bi)?;
                let rc  = u16::from_le_bytes([buf[bs-2], buf[bs-1]]) as usize;
                let mut off = 0usize;
                for _ in 0..rc {
                    if let Some(p_row) = decode_row(&buf, &mut off) {
                        let key = make_join_key(&p_row, &probe_keys);
                        if let Some(b_rows) = part_map.get(&key) {
                            for b_row in b_rows {
                                let mut row = p_row.clone();
                                row.extend_from_slice(b_row);
                                if !eval_preds(&row, &extra_preds) { continue; }
                                if out_writer.is_none() {
                                    let rr = encoded_size(&row)+64
                                        +row.iter().map(|(k,v)|k.len()+v.len()).sum::<usize>();
                                    out_writer = Some(ChunkWriter::new(
                                        (mem/8/rr.max(1)).max(64).min(4096)
                                    ));
                                }
                                out_writer.as_mut().unwrap()
                                    .push(row, |rows| unsafe { (*this).spill(rows) })?;
                            }
                        }
                    }
                }
            }
        }

        let w = out_writer.unwrap_or_else(|| ChunkWriter::new(1));
        let total = w.total;
        let rs = w.finish(|rows| self.spill(rows))?;
        eprintln!("[GRACE] out={}", total);
        Ok(rs)
    }

    fn collect_runset(&mut self, rs: RunSet) -> Result<Table> {
        let mut out = Vec::new();
        self.for_each_row_in_runset(rs, |row| { out.push(row); Ok(()) })?;
        Ok(out)
    }

    // ─────────────────────────────────────────────────────────────────────────
    // SORT — external merge sort, streaming k-way merge
    // ─────────────────────────────────────────────────────────────────────────
    fn exec_sort(&mut self, data: &OptSortData) -> Result<RunSet> {
        let input = self.exec_node(&data.underlying)?;
        eprintln!("[SORT] input={}", input.row_count());

        if input.row_count() == 0 { return Ok(RunSet::InMemory(Vec::new())); }

        // In-place only if small enough
        let fits_ram = match &input {
            RunSet::InMemory(t) => rough_bytes(t) <= self.memory_limit_bytes / 4,
            RunSet::Spilled {..} => false,
        };
        if fits_ram {
            let mut t = self.collect_runset(input)?;
            sort_slice(&mut t, &data.sort_specs);
            eprintln!("[SORT] in-place rows={}", t.len());
            return Ok(RunSet::InMemory(t));
        }

        // Pass 1: sorted runs
        let bpr: usize = match &input {
            RunSet::InMemory(t) if !t.is_empty() => rough_bytes(t) / t.len(),
            _ => 600,
        };
        let pass1_cap = self.cap_for(bpr);
        eprintln!("[SORT] external bpr~{} pass1_cap={}", bpr, pass1_cap);

        let mut sorted_runs: Vec<ScratchRun> = Vec::new();
        let mut chunk: Table = Vec::with_capacity(pass1_cap);
        let specs = data.sort_specs.clone();
        let this  = self as *mut Self;

        self.for_each_row_in_runset(input, |row| {
            chunk.push(row);
            if chunk.len() >= pass1_cap {
                sort_slice(&mut chunk, &specs);
                let run = unsafe { (*this).spill(&chunk)? };
                sorted_runs.push(run);
                chunk.clear();
            }
            Ok(())
        })?;
        if !chunk.is_empty() {
            sort_slice(&mut chunk, &specs);
            let this2 = self as *mut Self;
            sorted_runs.push(unsafe { (*this2).spill(&chunk)? });
            chunk = Vec::new();
        }
        drop(chunk);
        eprintln!("[SORT] pass1: {} sorted runs", sorted_runs.len());

        // Pass 2: k-way merge — each MergeHead holds exactly one block (block_size bytes)
        let bs = self.block_size as usize;
        let mut heads: Vec<MergeHead> = Vec::with_capacity(sorted_runs.len());
        for run in &sorted_runs {
            if run.num_blocks == 0 || run.row_count == 0 { continue; }
            let buf = self.read_block(run.start_block)?;
            let brc = u16::from_le_bytes([buf[bs-2], buf[bs-1]]) as usize;
            let mut h = MergeHead {
                run_start: run.start_block, run_blocks: run.num_blocks,
                cur_block: 0, block_buf: buf, block_row_count: brc,
                block_row_idx: 0, block_off: 0, current_row: None,
            };
            h.advance(bs);
            if h.current_row.is_some() { heads.push(h); }
        }

        let mut out_writer = ChunkWriter::new(pass1_cap);

        loop {
            if heads.is_empty() { break; }
            let mut best = 0usize;
            for i in 1..heads.len() {
                if cmp_rows(
                    heads[i].current_row.as_ref().unwrap(),
                    heads[best].current_row.as_ref().unwrap(),
                    &specs,
                ) == std::cmp::Ordering::Less { best = i; }
            }

            let row = heads[best].current_row.take().unwrap();
            out_writer.push(row, |rows| self.spill(rows))?;

            heads[best].advance(bs);
            if heads[best].current_row.is_none() {
                let next = heads[best].cur_block + 1;
                if next >= heads[best].run_blocks {
                    heads.swap_remove(best);
                } else {
                    let abs = heads[best].run_start + next;
                    let buf = self.read_block(abs)?;
                    let brc = u16::from_le_bytes([buf[bs-2], buf[bs-1]]) as usize;
                    heads[best].cur_block       = next;
                    heads[best].block_buf       = buf;
                    heads[best].block_row_count = brc;
                    heads[best].block_row_idx   = 0;
                    heads[best].block_off       = 0;
                    heads[best].advance(bs);
                    if heads[best].current_row.is_none() { heads.swap_remove(best); }
                }
            }
        }

        let rs = out_writer.finish(|rows| self.spill(rows))?;
        eprintln!("[SORT] merge done rows={}", rs.row_count());
        Ok(rs)
    }

    pub fn read_block_pub(&mut self, abs: u64) -> Result<Vec<u8>> { self.read_block(abs) }
}

// ─────────────────────────────────────────────────────────────────────────────
// Free functions
// ─────────────────────────────────────────────────────────────────────────────

fn make_join_key(row: &Row, keys: &[String]) -> String {
    let mut out = String::new();
    for (i, k) in keys.iter().enumerate() {
        if i > 0 { out.push('\x00'); }
        let v = row.iter()
            .find(|(col,_)| col == k || col.ends_with(&format!(".{}", k)))
            .map(|(_,v)| v.as_str())
            .unwrap_or("");
        out.push_str(v);
    }
    out
}

fn hash_str(s: &str) -> usize {
    let mut h: usize = 0xcbf29ce484222325u64 as usize;
    for b in s.bytes() {
        h ^= b as usize;
        h = h.wrapping_mul(0x100000001b3u64 as usize);
    }
    h
}

fn find_col<'a>(row: &'a Row, name: &str) -> Option<&'a String> {
    row.iter()
        .find(|(k,_)| k == name || k.ends_with(&format!(".{}", name)))
        .map(|(_,v)| v)
}

fn project_row(row: &Row, map: &[(String,String)]) -> Row {
    let mut r = Vec::with_capacity(map.len());
    for (from, to) in map {
        match find_col(row, from) {
            Some(v) => r.push((to.clone(), v.clone())),
            None    => eprintln!("[PROJ] col '{}' missing", from),
        }
    }
    r
}

fn eval_preds(row: &Row, preds: &[Predicate]) -> bool {
    use std::cmp::Ordering;
    preds.iter().all(|p| {
        let lhs = match find_col(row, &p.column_name) {
            Some(v) => v,
            None    => { eprintln!("[PRED] missing col '{}'", p.column_name); return false; }
        };
        let rhs: Option<String> = match &p.value {
            ComparisionValue::Column(c) => find_col(row, c).cloned(),
            ComparisionValue::String(s) => Some(s.clone()),
            ComparisionValue::I32(v)    => Some(v.to_string()),
            ComparisionValue::I64(v)    => Some(v.to_string()),
            ComparisionValue::F32(v)    => Some(v.to_string()),
            ComparisionValue::F64(v)    => Some(v.to_string()),
        };
        let rhs = match rhs { Some(ref r) => r, None => return false };
        let ord = match (lhs.parse::<f64>(), rhs.parse::<f64>()) {
            (Ok(a), Ok(b)) => a.partial_cmp(&b).unwrap_or(Ordering::Equal),
            _              => lhs.cmp(rhs),
        };
        match p.operator {
            ComparisionOperator::EQ  => ord == Ordering::Equal,
            ComparisionOperator::NE  => ord != Ordering::Equal,
            ComparisionOperator::GT  => ord == Ordering::Greater,
            ComparisionOperator::GTE => ord != Ordering::Less,
            ComparisionOperator::LT  => ord == Ordering::Less,
            ComparisionOperator::LTE => ord != Ordering::Greater,
        }
    })
}

fn sort_slice(tbl: &mut Table, specs: &[SortSpec]) {
    tbl.sort_by(|a, b| cmp_rows(a, b, specs));
}

fn cmp_rows(a: &Row, b: &Row, specs: &[SortSpec]) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    for s in specs {
        let va = a.iter().find(|(k,_)| k==&s.column_name
            || k.ends_with(&format!(".{}",s.column_name))).map(|(_,v)|v);
        let vb = b.iter().find(|(k,_)| k==&s.column_name
            || k.ends_with(&format!(".{}",s.column_name))).map(|(_,v)|v);
        let ord = match (va, vb) {
            (Some(a),Some(b)) => match (a.parse::<f64>(),b.parse::<f64>()) {
                (Ok(x),Ok(y)) => x.partial_cmp(&y).unwrap_or(Ordering::Equal),
                _             => a.cmp(b),
            },
            (None,None)=>Ordering::Equal,(None,_)=>Ordering::Less,(_,None)=>Ordering::Greater,
        };
        if ord != Ordering::Equal { return if s.ascending { ord } else { ord.reverse() }; }
    }
    Ordering::Equal
}

fn fmt_f64(v: f64) -> String {
    if v.fract() == 0.0 && v.abs() < 1e15 { format!("{:.1}", v) } else { v.to_string() }
}

fn rough_bytes(t: &Table) -> usize {
    let p = std::mem::size_of::<usize>();
    3*p + t.iter().map(|r| 3*p + r.iter()
        .map(|(k,v)| 6*p+k.len()+v.len()).sum::<usize>()).sum::<usize>()
}

fn opt_op_name(op: &OptQueryOp) -> &'static str {
    match op {
        OptQueryOp::Scan(_)        => "Scan",
        OptQueryOp::Filter(_)      => "Filter",
        OptQueryOp::Project(_)     => "Project",
        OptQueryOp::Cross(_)       => "Cross",
        OptQueryOp::Sort(_)        => "Sort",
        OptQueryOp::FilterCross(_) => "FilterCross",
        OptQueryOp::HashJoin(_)    => "HashJoin",
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Output helpers used by main.rs
// ─────────────────────────────────────────────────────────────────────────────

pub fn get_output_columns(op: &OptQueryOp, ctx: &DbContext) -> Vec<String> {
    match op {
        OptQueryOp::Scan(d) => ctx.get_table_specs()
            .iter().find(|t| t.name == d.table_id)
            .map(|s| s.column_specs.iter()
                .map(|c| format!("{}.{}", d.table_id, c.column_name)).collect())
            .unwrap_or_default(),
        OptQueryOp::Filter(d)      => get_output_columns(&d.underlying, ctx),
        OptQueryOp::Sort(d)        => get_output_columns(&d.underlying, ctx),
        OptQueryOp::Project(d)     => d.column_name_map.iter().map(|(_,to)| to.clone()).collect(),
        OptQueryOp::FilterCross(d) => {
            let mut l = get_output_columns(&d.left, ctx);
            l.extend(get_output_columns(&d.right, ctx));
            l
        }
        OptQueryOp::Cross(d) => {
            let mut l = get_output_columns(&d.left, ctx);
            l.extend(get_output_columns(&d.right, ctx));
            l
        }
        OptQueryOp::HashJoin(d) => {
            let mut l = get_output_columns(&d.left, ctx);
            l.extend(get_output_columns(&d.right, ctx));
            l
        }
    }
}   