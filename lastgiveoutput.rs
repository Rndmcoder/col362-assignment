// giveoutput.rs — streaming out-of-core query executor
//
// Core guarantee: no Vec<Row> ever holds more than chunk_cap rows.
// Operators exchange data via RunSet (either a small InMemory vec, or
// a list of on-disk ScratchRuns). The final result is also a RunSet;
// main.rs streams it out row-by-row without loading everything at once.

use anyhow::Result;
use std::io::{BufRead, Write};
use common::query::*;
use db_config::DbContext;
use common::DataType;

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
// RunSet — the universal result type for every operator
// ─────────────────────────────────────────────────────────────────────────────

pub enum RunSet {
    InMemory(Table),
    Spilled { runs: Vec<ScratchRun>, row_count: usize },
}

impl RunSet {
    pub fn row_count(&self) -> usize {
        match self {
            RunSet::InMemory(t)                 => t.len(),
            RunSet::Spilled { row_count, .. }   => *row_count,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ChunkWriter — helper that collects rows into a chunk and spills when full.
// Keeps all the chunk/spill bookkeeping in one place.
// ─────────────────────────────────────────────────────────────────────────────

struct ChunkWriter {
    chunk:     Table,
    runs:      Vec<ScratchRun>,
    cap:       usize,
    total:     usize,
}

impl ChunkWriter {
    fn new(cap: usize) -> Self {
        Self { chunk: Vec::with_capacity(cap), runs: Vec::new(), cap, total: 0 }
    }

    // Push one row; spill the chunk to disk if it reaches capacity.
    // `spill_fn` is a closure: |rows| -> Result<ScratchRun>
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

    // Finalise: spill leftover chunk (if any) and return RunSet.
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
// MergeHead — one read-head for k-way merge
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

    fn cap_for(&self, row_ram: usize) -> usize {
        let budget = self.memory_limit_bytes / 4;
        (budget / row_ram.max(1)).max(64).min(32_768)
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

    fn spill(&mut self, rows: &[Row]) -> Result<ScratchRun> {
        let bs     = self.block_size as usize;
        let usable = bs - 2;
        let mut payload = Vec::new();
        let mut cur = vec![0u8; bs];
        let mut off = 0usize;
        let mut cnt = 0u16;
        let mut enc = Vec::new();
        for row in rows {
            enc.clear();
            encode_row(row, &mut enc);
            if enc.len() > usable { anyhow::bail!("row too large: {}", enc.len()); }
            if off + enc.len() > usable {
                cur[bs-2] = (cnt & 0xFF) as u8; cur[bs-1] = (cnt >> 8) as u8;
                payload.extend_from_slice(&cur);
                cur.iter_mut().for_each(|b| *b = 0);
                off = 0; cnt = 0;
            }
            cur[off..off+enc.len()].copy_from_slice(&enc);
            off += enc.len(); cnt += 1;
        }
        cur[bs-2] = (cnt & 0xFF) as u8; cur[bs-1] = (cnt >> 8) as u8;
        payload.extend_from_slice(&cur);
        let n = (payload.len() / bs) as u64;
        let start = self.next_anon;
        self.next_anon += n;
        self.disk_out.write_all(format!("put block {} {}\n", start, n).as_bytes())?;
        self.disk_out.write_all(&payload)?;
        self.disk_out.flush()?;
        eprintln!("[SPILL] {} rows -> {} blocks (start={})", rows.len(), n, start);
        Ok(ScratchRun { start_block: start, num_blocks: n, row_count: rows.len() })
    }

    // ─────────────────────────────────────────────────────────────────────────
    // for_each_row — iterate every row in a RunSet, O(block_size) RAM.
    // Because Rust borrow checker won't let us have &mut self inside a closure
    // that also borrows self, we extract the run descriptors first.
    // ─────────────────────────────────────────────────────────────────────────
    fn for_each_row_in_runset<F>(&mut self, rs: RunSet, mut f: F) -> Result<()>
    where F: FnMut(Row) -> Result<()>
    {
        match rs {
            RunSet::InMemory(rows) => {
                for row in rows { f(row)?; }
            }
            RunSet::Spilled { runs, .. } => {
                // Collect (start, num_blocks) pairs — no borrow of self data.
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

    pub fn execute(&mut self, op: &QueryOp) -> Result<RunSet> {
        eprintln!("[EXEC] {}", op_name(op));
        self.exec_node(op)
    }

    fn exec_node(&mut self, op: &QueryOp) -> Result<RunSet> {
        match op {
            QueryOp::Scan(d)    => self.exec_scan(d),
            QueryOp::Filter(d)  => self.exec_filter(d),
            QueryOp::Project(d) => self.exec_project(d),
            QueryOp::Cross(d)   => self.exec_cross(d),
            QueryOp::Sort(d)    => self.exec_sort(d),
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // SCAN
    // ─────────────────────────────────────────────────────────────────────────
    fn exec_scan(&mut self, data: &ScanData) -> Result<RunSet> {
        let table_spec = self.ctx.get_table_specs()
            .iter().find(|t| t.name == data.table_id)
            .ok_or_else(|| anyhow::anyhow!("table not found: {}", data.table_id))?;
        let file_id = table_spec.file_id.clone();
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

                // Lazy-init writer on first valid row
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
    fn exec_filter(&mut self, data: &FilterData) -> Result<RunSet> {
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

                let budget = mem_limit / 4;
                let cap = (budget / row_ram.max(1)).max(64).min(32_768);

                writer = Some(ChunkWriter::new(cap));
            }

            let w = writer.as_mut().unwrap();
            w.push(row, |rows| unsafe { (*this).spill(rows) })
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
    fn exec_project(&mut self, data: &ProjectData) -> Result<RunSet> {
        let input = self.exec_node(&data.underlying)?;
        eprintln!("[PROJECT] input={}", input.row_count());

        let map = data.column_name_map.clone();
        let mut writer: Option<ChunkWriter> = None;

        let this = self as *mut Self;
        let mem_limit = self.memory_limit_bytes;

        self.for_each_row_in_runset(input, |row| {
            let r = project_row(&row, &map);   // ✅ correct operation

            if writer.is_none() && !r.is_empty() {
                let row_ram = encoded_size(&r) + 64
                    + r.iter().map(|(k,v)| k.len()+v.len()).sum::<usize>();

                let budget = mem_limit / 4;
                let cap = (budget / row_ram.max(1)).max(64).min(32_768);

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
    // CROSS
    // ─────────────────────────────────────────────────────────────────────────


    // fn exec_cross(&mut self, data: &CrossData) -> Result<RunSet> {
    //     let left_rs  = self.exec_node(&data.left)?;
    //     let right_rs = self.exec_node(&data.right)?;
    //     eprintln!("[CROSS] left={} right={}", left_rs.row_count(), right_rs.row_count());

    //     if left_rs.row_count() == 0 || right_rs.row_count() == 0 {
    //         return Ok(RunSet::InMemory(Vec::new()));
    //     }

    //     // Materialise the smaller side into RAM.
    //     let (outer_rs, inner): (RunSet, Table) =
    //         if right_rs.row_count() <= left_rs.row_count() {
    //             let inner = self.collect_runset(right_rs)?;
    //             (left_rs, inner)
    //         } else {
    //             let inner = self.collect_runset(left_rs)?;
    //             (right_rs, inner)
    //         };

    //     let inner_bytes = rough_bytes(&inner);
    //     if inner_bytes > self.memory_limit_bytes / 3 {
    //         anyhow::bail!("[CROSS] inner side too large: {} bytes", inner_bytes);
    //     }

    //     let mut writer = ChunkWriter::new(4096);

    //     let this = self as *mut Self;

    //     self.for_each_row_in_runset(outer_rs, |o_row| {
    //         for i_row in &inner {
    //             let mut row = o_row.clone();
    //             row.extend_from_slice(i_row);
    //             writer.push(row, |rows| unsafe { (*this).spill(rows) })?;
    //         }
    //         Ok(())
    //     })?;

    //     let rs = writer.finish(|rows| self.spill(rows))?;
    //     eprintln!("[CROSS] out={}", rs.row_count());
    //     Ok(rs)
    // }

    fn exec_cross(&mut self, data: &CrossData) -> Result<RunSet> {
        let left_rs  = self.exec_node(&data.left)?;
        let right_rs = self.exec_node(&data.right)?;
        eprintln!("[CROSS] left={} right={}", left_rs.row_count(), right_rs.row_count());

        if left_rs.row_count() == 0 || right_rs.row_count() == 0 {
            return Ok(RunSet::InMemory(Vec::new()));
        }

        // Budget: use 1/3 of memory for inner chunks, leave rest for output buffering
        let chunk_budget = self.memory_limit_bytes / 3;

        // Always stream outer, chunk the inner
        // Pick the smaller side as inner to minimise re-reads of inner chunks
        let (outer_rs, inner_rs) =
            if right_rs.row_count() <= left_rs.row_count() {
                (left_rs, right_rs)
            } else {
                (right_rs, left_rs)
            };

        // Collect inner into chunks that fit in chunk_budget
        let mut inner_chunks: Vec<Table> = Vec::new();
        {
            let mut current_chunk: Table = Vec::new();
            let mut current_bytes: usize = 0;

            let this = self as *mut Self;
            self.for_each_row_in_runset(inner_rs, |row| {
                let rb = rough_bytes(&vec![row.clone()]);
                if current_bytes + rb > chunk_budget && !current_chunk.is_empty() {
                    inner_chunks.push(std::mem::take(&mut current_chunk));
                    current_bytes = 0;
                }
                current_bytes += rb;
                current_chunk.push(row);
                Ok(())
            })?;
            if !current_chunk.is_empty() {
                inner_chunks.push(current_chunk);
            }
        }

        eprintln!("[CROSS] inner split into {} chunks", inner_chunks.len());

        // We need to iterate outer once per inner chunk.
        // Spill outer to disk so we can re-read it for each chunk.
        let outer_spilled: RunSet = match outer_rs {
            RunSet::InMemory(ref t) => {
                // Already in memory — just keep a reference; we'll clone below
                outer_rs
            }
            spilled => spilled,
        };

        // If only one inner chunk, single pass over outer
        let multi_pass = inner_chunks.len() > 1;

        // For multi-pass we need outer as a re-readable RunSet (spilled).
        // Force outer to spill if it isn't already and we need multiple passes.
        let outer_final: RunSet = if multi_pass {
            match outer_spilled {
                RunSet::Spilled { .. } => outer_spilled,
                RunSet::InMemory(rows) => {
                    // spill it
                    let mut cw = ChunkWriter::new(self.cap_for(600));
                    let this = self as *mut Self;
                    for row in rows {
                        cw.push(row, |r| unsafe { (*this).spill(r) })?;
                    }
                    cw.finish(|r| self.spill(r))?
                }
            }
        } else {
            outer_spilled
        };

        let mut out_writer = ChunkWriter::new(self.cap_for(600));
        let this = self as *mut Self;

        if !multi_pass {
            // Single pass — consume outer
            let chunk = &inner_chunks[0];
            self.for_each_row_in_runset(outer_final, |o_row| {
                for i_row in chunk {
                    let mut row = o_row.clone();
                    row.extend_from_slice(i_row);
                    out_writer.push(row, |rows| unsafe { (*this).spill(rows) })?;
                }
                Ok(())
            })?;
        } else {
            // Multi-pass: for each inner chunk, stream the whole outer
            // We need the run descriptors for manual re-reading
            let descriptors: Vec<(u64, u64)> = match &outer_final {
                RunSet::Spilled { runs, .. } => runs.iter()
                    .map(|r| (r.start_block, r.num_blocks))
                    .collect(),
                RunSet::InMemory(_) => unreachable!(),
            };
            let bs = self.block_size as usize;

            for chunk in &inner_chunks {
                for &(start, n_blk) in &descriptors {
                    for bi in 0..n_blk {
                        let buf = self.read_block(start + bi)?;
                        let b   = &buf[..];
                        let rc  = u16::from_le_bytes([b[bs-2], b[bs-1]]) as usize;
                        let mut off = 0usize;
                        for _ in 0..rc {
                            if let Some(o_row) = decode_row(&buf, &mut off) {
                                for i_row in chunk {
                                    let mut row = o_row.clone();
                                    row.extend_from_slice(i_row);
                                    out_writer.push(row, |rows| unsafe { (*this).spill(rows) })?;
                                }
                            }
                        }
                    }
                }
            }
        }

        let rs = out_writer.finish(|rows| self.spill(rows))?;
        eprintln!("[CROSS] out={}", rs.row_count());
        Ok(rs)
    }

    // Collect a RunSet into a Table — only use when you know it fits.
    fn collect_runset(&mut self, rs: RunSet) -> Result<Table> {
        let n = rs.row_count();
        let mut out = Vec::with_capacity(n);
        self.for_each_row_in_runset(rs, |row| { out.push(row); Ok(()) })?;
        Ok(out)
    }

    // ─────────────────────────────────────────────────────────────────────────
    // SORT — external merge sort, streaming k-way merge
    // ─────────────────────────────────────────────────────────────────────────
    fn exec_sort(&mut self, data: &SortData) -> Result<RunSet> {
        let input = self.exec_node(&data.underlying)?;
        eprintln!("[SORT] input={}", input.row_count());

        if input.row_count() == 0 { return Ok(RunSet::InMemory(Vec::new())); }

        // In-place sort only if already InMemory and fits comfortably.
        let fits_ram = match &input {
            RunSet::InMemory(t) => rough_bytes(t) <= self.memory_limit_bytes / 2,
            RunSet::Spilled {..} => false,
        };
        if fits_ram {
            let mut t = self.collect_runset(input)?;
            sort_slice(&mut t, &data.sort_specs);
            eprintln!("[SORT] in-place rows={}", t.len());
            return Ok(RunSet::InMemory(t));
        }

        // ── Pass 1: sorted runs ──────────────────────────────────────────────
        // Use a conservative bytes-per-row estimate.
        let bpr: usize = match &input {
            RunSet::InMemory(t) if !t.is_empty() => rough_bytes(t) / t.len(),
            _ => 600,
        };
        let pass1_cap = self.cap_for(bpr);
        eprintln!("[SORT] external bpr~{} pass1_cap={}", bpr, pass1_cap);

        let mut sorted_runs: Vec<ScratchRun> = Vec::new();
        let mut chunk: Table = Vec::with_capacity(pass1_cap);
        let specs = data.sort_specs.clone();

        let this = self as *mut Self;

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
            let this = self as *mut Self;
            sorted_runs.push(unsafe { (*this).spill(&chunk)? });
            chunk = Vec::new();
        }
        drop(chunk);
        eprintln!("[SORT] pass1: {} sorted runs", sorted_runs.len());

        // ── Pass 2: streaming k-way merge ────────────────────────────────────
        let total_rows: usize = sorted_runs.iter().map(|r| r.row_count).sum();
        let bs = self.block_size as usize;

        // Init one MergeHead per run.
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

            // Find minimum
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

            // Advance winning head
            heads[best].advance(bs);

            if heads[best].current_row.is_none() {
                let next = heads[best].cur_block + 1;
                if next >= heads[best].run_blocks {
                    heads.swap_remove(best);
                } else {
                    let abs = heads[best].run_start + next;
                    let buf = self.read_block(abs)?;
                    let brc = u16::from_le_bytes([buf[bs-2], buf[bs-1]]) as usize;
                    heads[best].cur_block = next;
                    heads[best].block_buf = buf;
                    heads[best].block_row_count = brc;
                    heads[best].block_row_idx   = 0;
                    heads[best].block_off       = 0;
                    heads[best].advance(bs);
                    if heads[best].current_row.is_none() {
                        heads.swap_remove(best);
                    }
                }
            }
        }

        let rs = out_writer.finish(|rows| self.spill(rows))?;
        eprintln!("[SORT] merge done rows={}", total_rows);
        Ok(rs)
    }

    // Public helpers for main.rs streaming
    pub fn read_block_pub(&mut self, abs: u64) -> Result<Vec<u8>> {
        self.read_block(abs)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Free functions
// ─────────────────────────────────────────────────────────────────────────────

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
            (Some(a), Some(b)) => match (a.parse::<f64>(), b.parse::<f64>()) {
                (Ok(x), Ok(y)) => x.partial_cmp(&y).unwrap_or(Ordering::Equal),
                _              => a.cmp(b),
            },
            (None,None) => Ordering::Equal, (None,_) => Ordering::Less, (_,None) => Ordering::Greater,
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
    3*p + t.iter().map(|r| 3*p + r.iter().map(|(k,v)| 6*p+k.len()+v.len()).sum::<usize>()).sum::<usize>()
}

fn op_name(op: &QueryOp) -> &'static str {
    match op { QueryOp::Scan(_)=>"Scan", QueryOp::Filter(_)=>"Filter",
               QueryOp::Project(_)=>"Project", QueryOp::Cross(_)=>"Cross", QueryOp::Sort(_)=>"Sort" }
}

// ─────────────────────────────────────────────────────────────────────────────
// Output helpers
// ─────────────────────────────────────────────────────────────────────────────

pub fn get_output_columns(op: &QueryOp, ctx: &DbContext) -> Vec<String> {
    match op {
        QueryOp::Scan(d) => ctx.get_table_specs()
            .iter().find(|t| t.name == d.table_id)
            .map(|s| s.column_specs.iter()
                .map(|c| format!("{}.{}", d.table_id, c.column_name)).collect())
            .unwrap_or_default(),
        QueryOp::Filter(d)  => get_output_columns(&d.underlying, ctx),
        QueryOp::Sort(d)    => get_output_columns(&d.underlying, ctx),
        QueryOp::Project(d) => d.column_name_map.iter().map(|(_,to)| to.clone()).collect(),
        QueryOp::Cross(d)   => {
            let mut l = get_output_columns(&d.left, ctx);
            l.extend(get_output_columns(&d.right, ctx));
            l
        }
    }
}