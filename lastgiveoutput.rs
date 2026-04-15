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
    2 + row.iter().map(|(k, v)| 4 + k.len() + v.len()).sum::<usize>()
}

pub fn decode_row(src: &[u8], off: &mut usize) -> Option<Row> {
    if *off + 2 > src.len() { return None; }
    let nc = u16::from_le_bytes([src[*off], src[*off + 1]]) as usize;
    *off += 2;
    let mut row = Vec::with_capacity(nc);
    for _ in 0..nc {
        if *off + 2 > src.len() { return None; }
        let kl = u16::from_le_bytes([src[*off], src[*off + 1]]) as usize;
        *off += 2;
        if *off + kl > src.len() { return None; }
        let k = String::from_utf8(src[*off..*off + kl].to_vec()).ok()?;
        *off += kl;
        if *off + 2 > src.len() { return None; }
        let vl = u16::from_le_bytes([src[*off], src[*off + 1]]) as usize;
        *off += 2;
        if *off + vl > src.len() { return None; }
        let v = String::from_utf8(src[*off..*off + vl].to_vec()).ok()?;
        *off += vl;
        row.push((k, v));
    }
    Some(row)
}

pub fn decode_row_pub(src: &[u8], off: &mut usize) -> Option<Row> {
    decode_row(src, off)
}

pub struct ScratchRun {
    pub start_block: u64,
    pub num_blocks:  u64,
    pub row_count:   usize,
}

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
        if self.block_row_idx >= self.block_row_count
            || self.block_off >= bs.saturating_sub(2)
        {
            self.current_row = None;
            return;
        }
        self.current_row = decode_row(&self.block_buf, &mut self.block_off);
        if self.current_row.is_some() {
            self.block_row_idx += 1;
        }
    }
}

// ── Per-partition streaming writer ────────────────────────────────────────────
// Holds exactly ONE block-sized buffer. Rows are encoded straight into it.
// When it fills, that block is written to disk and the buffer is cleared.
// Memory cost: block_size bytes per live writer — nothing else.
struct PartWriter {
    start_block: u64,
    num_blocks:  u64,
    row_count:   usize,
    buf:         Vec<u8>,  // block_size bytes, always
    buf_off:     usize,    // bytes written into buf so far (not counting footer)
    buf_cnt:     u16,      // rows written into the current block
}

impl PartWriter {
    fn new(start: u64, bs: usize) -> Self {
        PartWriter {
            start_block: start,
            num_blocks: 0,
            row_count: 0,
            buf: vec![0u8; bs],
            buf_off: 0,
            buf_cnt: 0,
        }
    }

    // Flush the current block to disk, then reset the buffer.
    // Caller must supply a write closure; we pass the absolute block number and data.
    fn flush<F: FnMut(u64, &[u8]) -> Result<()>>(&mut self, mut write: F) -> Result<()> {
        let bs = self.buf.len();
        self.buf[bs - 2] = (self.buf_cnt & 0xFF) as u8;
        self.buf[bs - 1] = (self.buf_cnt >> 8)   as u8;
        write(self.start_block + self.num_blocks, &self.buf)?;
        self.num_blocks += 1;
        // Zero the buffer and reset counters.
        for b in &mut self.buf { *b = 0; }
        self.buf_off = 0;
        self.buf_cnt = 0;
        Ok(())
    }

    // Push an already-encoded row (enc) into the writer, flushing if needed.
    fn push_encoded<F: FnMut(u64, &[u8]) -> Result<()>>(
        &mut self,
        enc:   &[u8],
        write: F,
    ) -> Result<()> {
        let usable = self.buf.len() - 2;
        if enc.len() > usable {
            anyhow::bail!(
                "encoded row ({} bytes) exceeds usable block space ({})",
                enc.len(), usable
            );
        }
        // Flush if the row won't fit in what's left.
        if self.buf_off + enc.len() > usable {
            self.flush(write)?;
        }
        // Now copy into buf using only local variables — no double-borrow.
        let start = self.buf_off;
        let end   = start + enc.len();
        self.buf[start..end].copy_from_slice(enc);
        self.buf_off += enc.len();
        self.buf_cnt += 1;
        self.row_count += 1;
        Ok(())
    }

    // Finish: flush any partial block, return (start, num_blocks, row_count).
    fn finish<F: FnMut(u64, &[u8]) -> Result<()>>(
        &mut self,
        write: F,
    ) -> Result<(u64, u64, usize)> {
        if self.buf_cnt > 0 {
            self.flush(write)?;
        }
        Ok((self.start_block, self.num_blocks, self.row_count))
    }
}

// ─────────────────────────────────────────────────────────────────────────────

pub struct Executor<'a, R: BufRead, W: Write> {
    disk_in:                R,
    disk_out:               &'a mut W,
    pub block_size:         u64,
    ctx:                    &'a DbContext,
    next_anon:              u64,
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
            disk_in,
            disk_out,
            block_size,
            ctx,
            next_anon: anon_start,
            memory_limit_bytes: 20 * 1024 * 1024,
        })
    }

    pub fn set_memory_limit_mb(&mut self, mb: u32) {
        let overhead = 28 * 1024 * 1024usize;
        let total    = (mb as usize) * 1024 * 1024;
        self.memory_limit_bytes = total.saturating_sub(overhead).max(4 * 1024 * 1024);
        eprintln!(
            "[INIT] memory_limit_bytes={} ({} MB data, {} MB total)",
            self.memory_limit_bytes,
            self.memory_limit_bytes / (1024 * 1024),
            mb
        );
    }

    // How many rows to buffer before spilling (1/8 of budget).
    fn cap_for(&self, row_ram: usize) -> usize {
        let budget = self.memory_limit_bytes / 8;
        (budget / row_ram.max(1)).max(64).min(2048)
    }

    // ── Raw disk I/O ──────────────────────────────────────────────────────────

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
            buf[n..n + tc].copy_from_slice(&a[..tc]);
            self.disk_in.consume(tc);
            n += tc;
        }
        Ok(buf)
    }

    fn write_block_raw(&mut self, abs: u64, data: &[u8]) -> Result<()> {
        self.disk_out.write_all(format!("put block {} 1\n", abs).as_bytes())?;
        self.disk_out.write_all(data)?;
        self.disk_out.flush()?;
        Ok(())
    }

    fn alloc_blocks(&mut self, n: u64) -> u64 {
        let s = self.next_anon;
        self.next_anon += n;
        s
    }

    // ── Spill a slice of rows → one or more blocks ────────────────────────────

    fn spill(&mut self, rows: &[Row]) -> Result<ScratchRun> {
        if rows.is_empty() {
            return Ok(ScratchRun {
                start_block: self.next_anon,
                num_blocks: 0,
                row_count: 0,
            });
        }
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
            if enc.len() > usable {
                anyhow::bail!("row too large: {} (usable={})", enc.len(), usable);
            }
            if off + enc.len() > usable {
                cur[bs - 2] = (cnt & 0xFF) as u8;
                cur[bs - 1] = (cnt >> 8)   as u8;
                self.write_block_raw(start + n_blocks, &cur)?;
                n_blocks += 1;
                for b in &mut cur { *b = 0; }
                off = 0;
                cnt = 0;
            }
            cur[off..off + enc.len()].copy_from_slice(&enc);
            off += enc.len();
            cnt += 1;
        }
        cur[bs - 2] = (cnt & 0xFF) as u8;
        cur[bs - 1] = (cnt >> 8)   as u8;
        self.write_block_raw(start + n_blocks, &cur)?;
        n_blocks += 1;
        self.next_anon += n_blocks;
        eprintln!("[SPILL] {} rows -> {} blocks (start={})", rows.len(), n_blocks, start);
        Ok(ScratchRun { start_block: start, num_blocks: n_blocks, row_count: rows.len() })
    }

    // ── Output accumulator ────────────────────────────────────────────────────

    fn emit_row(
        &mut self,
        row:       Row,
        out_buf:   &mut Vec<Row>,
        out_runs:  &mut Vec<ScratchRun>,
        out_cap:   &mut usize,
        out_total: &mut usize,
    ) -> Result<()> {
        if *out_cap == 0 {
            let rr = encoded_size(&row) + 64
                + row.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>();
            *out_cap = self.cap_for(rr);
            out_buf.reserve(*out_cap);
        }
        *out_total += 1;
        out_buf.push(row);
        if out_buf.len() >= *out_cap {
            let run = self.spill(out_buf)?;
            out_runs.push(run);
            out_buf.clear();
        }
        Ok(())
    }

    fn finalize_output(
        &mut self,
        mut out_buf:  Vec<Row>,
        mut out_runs: Vec<ScratchRun>,
        out_total:    usize,
    ) -> Result<RunSet> {
        if out_runs.is_empty() {
            return Ok(RunSet::InMemory(out_buf));
        }
        if !out_buf.is_empty() {
            let run = self.spill(&out_buf)?;
            out_runs.push(run);
            out_buf.clear();
        }
        Ok(RunSet::Spilled { row_count: out_total, runs: out_runs })
    }

    fn ensure_on_disk(&mut self, rs: RunSet) -> Result<Vec<(u64, u64)>> {
        match rs {
            RunSet::Spilled { runs, .. } => {
                Ok(runs.into_iter().map(|r| (r.start_block, r.num_blocks)).collect())
            }
            RunSet::InMemory(rows) => {
                if rows.is_empty() { return Ok(vec![]); }
                let cap = self.cap_for(600);
                let mut descs = Vec::new();
                let mut i = 0;
                while i < rows.len() {
                    let end = (i + cap).min(rows.len());
                    let run = self.spill(&rows[i..end])?;
                    descs.push((run.start_block, run.num_blocks));
                    i = end;
                }
                Ok(descs)
            }
        }
    }

    fn collect_runset(&mut self, rs: RunSet) -> Result<Table> {
        let hint = rs.row_count().min(65536);
        let mut out = Vec::with_capacity(hint);
        match rs {
            RunSet::InMemory(rows) => { out = rows; }
            RunSet::Spilled { runs, .. } => {
                let descs: Vec<(u64, u64)> = runs.iter()
                    .map(|r| (r.start_block, r.num_blocks))
                    .collect();
                let bs = self.block_size as usize;
                for (start, n_blk) in descs {
                    for bi in 0..n_blk {
                        let buf = self.read_block(start + bi)?;
                        let rc  = u16::from_le_bytes([buf[bs - 2], buf[bs - 1]]) as usize;
                        let mut off = 0usize;
                        for _ in 0..rc {
                            if let Some(row) = decode_row(&buf, &mut off) {
                                out.push(row);
                            }
                        }
                    }
                }
            }
        }
        Ok(out)
    }

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

    // ── SCAN ─────────────────────────────────────────────────────────────────

    fn exec_scan(&mut self, data: &ScanData) -> Result<RunSet> {
        let table_spec = self.ctx.get_table_specs()
            .iter()
            .find(|t| t.name == data.table_id)
            .ok_or_else(|| anyhow::anyhow!("table not found: {}", data.table_id))?;
        let file_id   = table_spec.file_id.clone();
        let col_specs = table_spec.column_specs.clone();
        let table_id  = data.table_id.clone();

        let mut line = String::new();
        self.disk_out
            .write_all(format!("get file start-block {}\n", file_id).as_bytes())?;
        self.disk_out.flush()?;
        self.disk_in.read_line(&mut line)?;
        let file_start: u64 = line.trim().parse()
            .map_err(|e| anyhow::anyhow!("file start-block: '{}': {}", line.trim(), e))?;
        line.clear();

        self.disk_out
            .write_all(format!("get file num-blocks {}\n", file_id).as_bytes())?;
        self.disk_out.flush()?;
        self.disk_in.read_line(&mut line)?;
        let file_blocks: u64 = line.trim().parse()
            .map_err(|e| anyhow::anyhow!("file num-blocks: '{}': {}", line.trim(), e))?;
        line.clear();

        eprintln!("[SCAN] '{}' start={} blocks={}", table_id, file_start, file_blocks);

        let bs = self.block_size as usize;
        let mut out_buf:  Vec<Row>        = Vec::new();
        let mut out_runs: Vec<ScratchRun> = Vec::new();
        let mut out_cap   = 0usize;
        let mut out_total = 0usize;

        for blk_rel in 0..file_blocks {
            let blk = self.read_block(file_start + blk_rel)?;
            let b   = &blk[..];
            let rc  = u16::from_le_bytes([b[bs - 2], b[bs - 1]]) as usize;
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
                            if off + 4 > bs - 2 { ok = false; break; }
                            let v = i32::from_le_bytes(b[off..off + 4].try_into().unwrap());
                            off += 4;
                            v.to_string()
                        }
                        DataType::Int64 => {
                            if off + 8 > bs - 2 { ok = false; break; }
                            let v = i64::from_le_bytes(b[off..off + 8].try_into().unwrap());
                            off += 8;
                            v.to_string()
                        }
                        DataType::Float32 => {
                            if off + 4 > bs - 2 { ok = false; break; }
                            let v = f32::from_le_bytes(b[off..off + 4].try_into().unwrap());
                            off += 4;
                            if !v.is_finite() { ok = false; break; }
                            fmt_f64(v as f64)
                        }
                        DataType::Float64 => {
                            if off + 8 > bs - 2 { ok = false; break; }
                            let v = f64::from_le_bytes(b[off..off + 8].try_into().unwrap());
                            off += 8;
                            if !v.is_finite() { ok = false; break; }
                            fmt_f64(v)
                        }
                        DataType::String => {
                            let s0 = off;
                            while off < bs - 2 && b[off] != 0 { off += 1; }
                            if off >= bs - 2 { ok = false; break; }
                            let s = String::from_utf8_lossy(&b[s0..off]).to_string();
                            off += 1;
                            s
                        }
                        _ => { ok = false; break; }
                    };
                    row.push((format!("{}.{}", table_id, col.column_name), val));
                }

                if !ok || row.len() != col_specs.len() {
                    eprintln!("[SCAN] bad row blk={} off={}", file_start + blk_rel, row_start);
                    break 'row_loop;
                }
                self.emit_row(row, &mut out_buf, &mut out_runs, &mut out_cap, &mut out_total)?;
            }
        }

        let rs = self.finalize_output(out_buf, out_runs, out_total)?;
        eprintln!("[SCAN] done total={}", out_total);
        Ok(rs)
    }

    // ── FILTER ───────────────────────────────────────────────────────────────

    fn exec_filter(&mut self, data: &OptFilterData) -> Result<RunSet> {
        let input = self.exec_node(&data.underlying)?;
        eprintln!("[FILTER] input={}", input.row_count());
        let preds = data.predicates.clone();

        let mut out_buf:  Vec<Row>        = Vec::new();
        let mut out_runs: Vec<ScratchRun> = Vec::new();
        let mut out_cap   = 0usize;
        let mut out_total = 0usize;

        match input {
            RunSet::InMemory(rows) => {
                for row in rows {
                    if eval_preds(&row, &preds) {
                        self.emit_row(
                            row, &mut out_buf, &mut out_runs, &mut out_cap, &mut out_total,
                        )?;
                    }
                }
            }
            RunSet::Spilled { runs, .. } => {
                let descs: Vec<(u64, u64)> = runs.iter()
                    .map(|r| (r.start_block, r.num_blocks))
                    .collect();
                let bs = self.block_size as usize;
                for (start, n_blk) in descs {
                    for bi in 0..n_blk {
                        let buf = self.read_block(start + bi)?;
                        let rc  = u16::from_le_bytes([buf[bs - 2], buf[bs - 1]]) as usize;
                        let mut off = 0usize;
                        for _ in 0..rc {
                            if let Some(row) = decode_row(&buf, &mut off) {
                                if eval_preds(&row, &preds) {
                                    self.emit_row(
                                        row, &mut out_buf, &mut out_runs,
                                        &mut out_cap, &mut out_total,
                                    )?;
                                }
                            }
                        }
                    }
                }
            }
        }

        let rs = self.finalize_output(out_buf, out_runs, out_total)?;
        eprintln!("[FILTER] out={}", out_total);
        Ok(rs)
    }

    // ── PROJECT ──────────────────────────────────────────────────────────────

    fn exec_project(&mut self, data: &OptProjectData) -> Result<RunSet> {
        let input = self.exec_node(&data.underlying)?;
        eprintln!("[PROJECT] input={}", input.row_count());
        let map = data.column_name_map.clone();

        let mut out_buf:  Vec<Row>        = Vec::new();
        let mut out_runs: Vec<ScratchRun> = Vec::new();
        let mut out_cap   = 0usize;
        let mut out_total = 0usize;

        match input {
            RunSet::InMemory(rows) => {
                for row in rows {
                    let r = project_row(&row, &map);
                    if !r.is_empty() {
                        self.emit_row(
                            r, &mut out_buf, &mut out_runs, &mut out_cap, &mut out_total,
                        )?;
                    }
                }
            }
            RunSet::Spilled { runs, .. } => {
                let descs: Vec<(u64, u64)> = runs.iter()
                    .map(|r| (r.start_block, r.num_blocks))
                    .collect();
                let bs = self.block_size as usize;
                for (start, n_blk) in descs {
                    for bi in 0..n_blk {
                        let buf = self.read_block(start + bi)?;
                        let rc  = u16::from_le_bytes([buf[bs - 2], buf[bs - 1]]) as usize;
                        let mut off = 0usize;
                        for _ in 0..rc {
                            if let Some(row) = decode_row(&buf, &mut off) {
                                let r = project_row(&row, &map);
                                if !r.is_empty() {
                                    self.emit_row(
                                        r, &mut out_buf, &mut out_runs,
                                        &mut out_cap, &mut out_total,
                                    )?;
                                }
                            }
                        }
                    }
                }
            }
        }

        let rs = self.finalize_output(out_buf, out_runs, out_total)?;
        eprintln!("[PROJECT] out={}", out_total);
        Ok(rs)
    }

    // ── CROSS ────────────────────────────────────────────────────────────────

    fn exec_cross(&mut self, data: &OptCrossData) -> Result<RunSet> {
        let left_rs  = self.exec_node(&data.left)?;
        let right_rs = self.exec_node(&data.right)?;
        eprintln!("[CROSS] left={} right={}", left_rs.row_count(), right_rs.row_count());

        if left_rs.row_count() == 0 || right_rs.row_count() == 0 {
            return Ok(RunSet::InMemory(Vec::new()));
        }

        let (outer_rs, inner_rs) =
            if right_rs.row_count() <= left_rs.row_count() {
                (left_rs, right_rs)
            } else {
                (right_rs, left_rs)
            };

        let chunk_budget = self.memory_limit_bytes / 4;
        let p  = std::mem::size_of::<usize>();
        let bs = self.block_size as usize;

        let inner_descs = self.ensure_on_disk(inner_rs)?;
        let outer_descs = self.ensure_on_disk(outer_rs)?;

        let mut out_buf:    Vec<Row>        = Vec::new();
        let mut out_runs:   Vec<ScratchRun> = Vec::new();
        let mut out_cap     = 0usize;
        let mut out_total   = 0usize;
        let mut chunk:      Vec<Row>        = Vec::new();
        let mut chunk_bytes = 0usize;

        for (i_start, i_nblk) in &inner_descs {
            for bi in 0..*i_nblk {
                let ibuf = self.read_block(*i_start + bi)?;
                let rc   = u16::from_le_bytes([ibuf[bs - 2], ibuf[bs - 1]]) as usize;
                let mut off = 0usize;
                for _ in 0..rc {
                    if let Some(row) = decode_row(&ibuf, &mut off) {
                        let rb = 3 * p
                            + row.iter().map(|(k, v)| 6 * p + k.len() + v.len()).sum::<usize>();
                        if chunk_bytes + rb > chunk_budget && !chunk.is_empty() {
                            for &(o_start, o_nblk) in &outer_descs {
                                for bo in 0..o_nblk {
                                    let obuf = self.read_block(o_start + bo)?;
                                    let orc  = u16::from_le_bytes(
                                        [obuf[bs - 2], obuf[bs - 1]]
                                    ) as usize;
                                    let mut ooff = 0usize;
                                    for _ in 0..orc {
                                        if let Some(o_row) = decode_row(&obuf, &mut ooff) {
                                            for i_row in &chunk {
                                                let mut r = o_row.clone();
                                                r.extend_from_slice(i_row);
                                                self.emit_row(
                                                    r, &mut out_buf, &mut out_runs,
                                                    &mut out_cap, &mut out_total,
                                                )?;
                                            }
                                        }
                                    }
                                }
                            }
                            chunk.clear();
                            chunk_bytes = 0;
                        }
                        chunk_bytes += rb;
                        chunk.push(row);
                    }
                }
            }
        }
        if !chunk.is_empty() {
            for &(o_start, o_nblk) in &outer_descs {
                for bo in 0..o_nblk {
                    let obuf = self.read_block(o_start + bo)?;
                    let orc  = u16::from_le_bytes([obuf[bs - 2], obuf[bs - 1]]) as usize;
                    let mut ooff = 0usize;
                    for _ in 0..orc {
                        if let Some(o_row) = decode_row(&obuf, &mut ooff) {
                            for i_row in &chunk {
                                let mut r = o_row.clone();
                                r.extend_from_slice(i_row);
                                self.emit_row(
                                    r, &mut out_buf, &mut out_runs, &mut out_cap, &mut out_total,
                                )?;
                            }
                        }
                    }
                }
            }
        }

        let rs = self.finalize_output(out_buf, out_runs, out_total)?;
        eprintln!("[CROSS] out={}", rs.row_count());
        Ok(rs)
    }

    // ── FILTER_CROSS ─────────────────────────────────────────────────────────

    fn exec_filter_cross(&mut self, data: &FilterCrossData) -> Result<RunSet> {
        let left_rs  = self.exec_node(&data.left)?;
        let right_rs = self.exec_node(&data.right)?;
        eprintln!(
            "[FILTER_CROSS] left={} right={} preds={}",
            left_rs.row_count(), right_rs.row_count(), data.predicates.len()
        );

        if left_rs.row_count() == 0 || right_rs.row_count() == 0 {
            return Ok(RunSet::InMemory(Vec::new()));
        }

        let preds        = data.predicates.clone();
        let chunk_budget = self.memory_limit_bytes / 4;
        let p  = std::mem::size_of::<usize>();
        let bs = self.block_size as usize;

        let (outer_rs, inner_rs) =
            if right_rs.row_count() <= left_rs.row_count() {
                (left_rs, right_rs)
            } else {
                (right_rs, left_rs)
            };

        let inner_descs = self.ensure_on_disk(inner_rs)?;
        let outer_descs = self.ensure_on_disk(outer_rs)?;

        let mut out_buf:    Vec<Row>        = Vec::new();
        let mut out_runs:   Vec<ScratchRun> = Vec::new();
        let mut out_cap     = 0usize;
        let mut out_total   = 0usize;
        let mut chunk:      Vec<Row>        = Vec::new();
        let mut chunk_bytes = 0usize;

        for (i_start, i_nblk) in &inner_descs {
            for bi in 0..*i_nblk {
                let ibuf = self.read_block(*i_start + bi)?;
                let rc   = u16::from_le_bytes([ibuf[bs - 2], ibuf[bs - 1]]) as usize;
                let mut off = 0usize;
                for _ in 0..rc {
                    if let Some(row) = decode_row(&ibuf, &mut off) {
                        let rb = 3 * p
                            + row.iter().map(|(k, v)| 6 * p + k.len() + v.len()).sum::<usize>();
                        if chunk_bytes + rb > chunk_budget && !chunk.is_empty() {
                            for &(o_start, o_nblk) in &outer_descs {
                                for bo in 0..o_nblk {
                                    let obuf = self.read_block(o_start + bo)?;
                                    let orc  = u16::from_le_bytes(
                                        [obuf[bs - 2], obuf[bs - 1]]
                                    ) as usize;
                                    let mut ooff = 0usize;
                                    for _ in 0..orc {
                                        if let Some(o_row) = decode_row(&obuf, &mut ooff) {
                                            for i_row in &chunk {
                                                let mut r = o_row.clone();
                                                r.extend_from_slice(i_row);
                                                if !eval_preds(&r, &preds) { continue; }
                                                self.emit_row(
                                                    r, &mut out_buf, &mut out_runs,
                                                    &mut out_cap, &mut out_total,
                                                )?;
                                            }
                                        }
                                    }
                                }
                            }
                            chunk.clear();
                            chunk_bytes = 0;
                        }
                        chunk_bytes += rb;
                        chunk.push(row);
                    }
                }
            }
        }
        if !chunk.is_empty() {
            for &(o_start, o_nblk) in &outer_descs {
                for bo in 0..o_nblk {
                    let obuf = self.read_block(o_start + bo)?;
                    let orc  = u16::from_le_bytes([obuf[bs - 2], obuf[bs - 1]]) as usize;
                    let mut ooff = 0usize;
                    for _ in 0..orc {
                        if let Some(o_row) = decode_row(&obuf, &mut ooff) {
                            for i_row in &chunk {
                                let mut r = o_row.clone();
                                r.extend_from_slice(i_row);
                                if !eval_preds(&r, &preds) { continue; }
                                self.emit_row(
                                    r, &mut out_buf, &mut out_runs, &mut out_cap, &mut out_total,
                                )?;
                            }
                        }
                    }
                }
            }
        }

        let rs = self.finalize_output(out_buf, out_runs, out_total)?;
        eprintln!("[FILTER_CROSS] out={}", out_total);
        Ok(rs)
    }

    // ── HASH JOIN ─────────────────────────────────────────────────────────────

    fn exec_hash_join(&mut self, data: &HashJoinData) -> Result<RunSet> {
        let left_rs  = self.exec_node(&data.left)?;
        let right_rs = self.exec_node(&data.right)?;
        eprintln!(
            "[HASHJOIN] left={} right={} keys={:?}={:?}",
            left_rs.row_count(), right_rs.row_count(),
            data.left_keys, data.right_keys
        );

        if left_rs.row_count() == 0 || right_rs.row_count() == 0 {
            return Ok(RunSet::InMemory(Vec::new()));
        }

        let extra_preds = data.extra_preds.clone();
        let left_keys   = data.left_keys.clone();
        let right_keys  = data.right_keys.clone();

        let (build_rs, probe_rs, build_keys, probe_keys) =
            if left_rs.row_count() <= right_rs.row_count() {
                (left_rs, right_rs, left_keys, right_keys)
            } else {
                (right_rs, left_rs, right_keys, left_keys)
            };

        let build_count  = build_rs.row_count();
        let build_budget = self.memory_limit_bytes / 4;
        let p = std::mem::size_of::<usize>();
        eprintln!(
            "[HASHJOIN] build={} probe={} budget={}MB",
            build_count, probe_rs.row_count(), build_budget / (1024 * 1024)
        );

        let mut build_map: HashMap<String, Vec<Row>> =
            HashMap::with_capacity(build_count.min(16384));
        let mut build_bytes = 0usize;
        let mut overflowed  = false;

        let overflow_cap = self.cap_for(600);
        let mut overflow_buf:  Vec<Row>        = Vec::new();
        let mut overflow_runs: Vec<ScratchRun> = Vec::new();
        let mut overflow_total = 0usize;

        let build_descs = self.ensure_on_disk(build_rs)?;
        let bs = self.block_size as usize;

        for (start, n_blk) in &build_descs {
            for bi in 0..*n_blk {
                let buf = self.read_block(*start + bi)?;
                let rc  = u16::from_le_bytes([buf[bs - 2], buf[bs - 1]]) as usize;
                let mut off = 0usize;
                for _ in 0..rc {
                    if let Some(row) = decode_row(&buf, &mut off) {
                        if overflowed {
                            overflow_total += 1;
                            overflow_buf.push(row);
                            if overflow_buf.len() >= overflow_cap {
                                let r = self.spill(&overflow_buf)?;
                                overflow_runs.push(r);
                                overflow_buf.clear();
                            }
                        } else {
                            let key = make_join_key(&row, &build_keys);
                            let rb  = 3 * p
                                + row.iter()
                                    .map(|(k, v)| 6 * p + k.len() + v.len())
                                    .sum::<usize>()
                                + key.len() + p;
                            build_bytes += rb;
                            if build_bytes > build_budget {
                                overflowed = true;
                                overflow_total += 1;
                                overflow_buf.push(row);
                            } else {
                                build_map.entry(key).or_default().push(row);
                            }
                        }
                    }
                }
            }
        }

        if overflowed {
            if !overflow_buf.is_empty() {
                let r = self.spill(&overflow_buf)?;
                overflow_runs.push(r);
                overflow_buf.clear();
            }
            // Drain the in-memory build map to disk.
            let map_cap = self.cap_for(600);
            let mut map_buf:  Vec<Row>        = Vec::new();
            let mut map_runs: Vec<ScratchRun> = Vec::new();
            let mut map_total = 0usize;
            for rows in build_map.drain().map(|(_, v)| v) {
                for row in rows {
                    map_total += 1;
                    map_buf.push(row);
                    if map_buf.len() >= map_cap {
                        let r = self.spill(&map_buf)?;
                        map_runs.push(r);
                        map_buf.clear();
                    }
                }
            }
            if !map_buf.is_empty() {
                let r = self.spill(&map_buf)?;
                map_runs.push(r);
            }
            let total_build = map_total + overflow_total;
            let all_runs: Vec<ScratchRun> = map_runs
                .into_iter()
                .chain(overflow_runs.into_iter())
                .collect();
            let all_build_rs = RunSet::Spilled { row_count: total_build, runs: all_runs };
            eprintln!("[HASHJOIN] overflow → grace (build={})", total_build);
            return self.grace_hash_join(
                all_build_rs, probe_rs, build_keys, probe_keys, extra_preds,
            );
        }

        eprintln!("[HASHJOIN] build map: {} keys, {} bytes", build_map.len(), build_bytes);

        let probe_descs = self.ensure_on_disk(probe_rs)?;
        let mut out_buf:  Vec<Row>        = Vec::new();
        let mut out_runs: Vec<ScratchRun> = Vec::new();
        let mut out_cap   = 0usize;
        let mut out_total = 0usize;

        for (start, n_blk) in probe_descs {
            for bi in 0..n_blk {
                let buf = self.read_block(start + bi)?;
                let rc  = u16::from_le_bytes([buf[bs - 2], buf[bs - 1]]) as usize;
                let mut off = 0usize;
                for _ in 0..rc {
                    if let Some(p_row) = decode_row(&buf, &mut off) {
                        let key = make_join_key(&p_row, &probe_keys);
                        if let Some(build_rows) = build_map.get(&key) {
                            for b_row in build_rows {
                                let mut row = p_row.clone();
                                row.extend_from_slice(b_row);
                                if !eval_preds(&row, &extra_preds) { continue; }
                                self.emit_row(
                                    row, &mut out_buf, &mut out_runs, &mut out_cap, &mut out_total,
                                )?;
                            }
                        }
                    }
                }
            }
        }

        let rs = self.finalize_output(out_buf, out_runs, out_total)?;
        eprintln!("[HASHJOIN] out={}", out_total);
        Ok(rs)
    }

    // ── GRACE HASH JOIN ───────────────────────────────────────────────────────
    // Peak RAM: n_parts * block_size bytes for writers (one buffer each).
    // With n_parts=16, block_size=4096 → 64 KB.  Build and probe writers
    // are NOT live at the same time; build writers are dropped before probe
    // writers are created.

    fn grace_hash_join(
        &mut self,
        build_rs:    RunSet,
        probe_rs:    RunSet,
        build_keys:  Vec<String>,
        probe_keys:  Vec<String>,
        extra_preds: Vec<Predicate>,
    ) -> Result<RunSet> {
        let n_parts: usize = 16;
        let bs = self.block_size as usize;
        eprintln!("[GRACE] partitioning into {} buckets", n_parts);

        // How many blocks to reserve per partition.
        // For 150 k rows at ~160 bytes/row encoded → ~24 000 blocks total
        // → ~1 500 per partition.  We reserve 4 096 to be very safe.
        let per_part_blks: u64 = 4096;

        // ── Partition BUILD side ──────────────────────────────────────────────
        let mut bw: Vec<PartWriter> = (0..n_parts)
            .map(|_| {
                let start = self.alloc_blocks(per_part_blks);
                PartWriter::new(start, bs)
            })
            .collect();

        let build_descs = self.ensure_on_disk(build_rs)?;
        let mut enc = Vec::with_capacity(1024);

        for (start, n_blk) in &build_descs {
            for bi in 0..*n_blk {
                let buf = self.read_block(*start + bi)?;
                let rc  = u16::from_le_bytes([buf[bs - 2], buf[bs - 1]]) as usize;
                let mut off = 0usize;
                for _ in 0..rc {
                    if let Some(row) = decode_row(&buf, &mut off) {
                        let key = make_join_key(&row, &build_keys);
                        let pi  = hash_str(&key) % n_parts;
                        enc.clear();
                        encode_row(&row, &mut enc);
                        // push_encoded handles the flush internally — no double-borrow.
                        let enc_snap = enc.clone(); // clone is tiny (< block_size)
                        bw[pi].push_encoded(&enc_snap, |abs, data| {
                            self.write_block_raw(abs, data)
                        })?;
                    }
                }
            }
        }
        // Flush partial blocks for all build writers.
        for i in 0..n_parts {
            if bw[i].buf_cnt > 0 {
                let abs      = bw[i].start_block + bw[i].num_blocks;
                let buf_cnt  = bw[i].buf_cnt;
                let buf_len  = bw[i].buf.len();
                bw[i].buf[buf_len - 2] = (buf_cnt & 0xFF) as u8;
                bw[i].buf[buf_len - 1] = (buf_cnt >> 8)   as u8;
                let data = bw[i].buf.clone();
                self.write_block_raw(abs, &data)?;
                bw[i].num_blocks += 1;
                bw[i].buf_cnt = 0;
            }
        }
        let build_parts: Vec<(u64, u64, usize)> = bw.into_iter()
            .map(|w| (w.start_block, w.num_blocks, w.row_count))
            .collect();
        // bw dropped here → 16 * 4096 = 64 KB freed.

        // ── Partition PROBE side ──────────────────────────────────────────────
        let probe_per_part_blks: u64 = per_part_blks * 10; // probe side is larger
        let mut pw: Vec<PartWriter> = (0..n_parts)
            .map(|_| {
                let start = self.alloc_blocks(probe_per_part_blks);
                PartWriter::new(start, bs)
            })
            .collect();

        let probe_descs = self.ensure_on_disk(probe_rs)?;

        for (start, n_blk) in &probe_descs {
            for bi in 0..*n_blk {
                let buf = self.read_block(*start + bi)?;
                let rc  = u16::from_le_bytes([buf[bs - 2], buf[bs - 1]]) as usize;
                let mut off = 0usize;
                for _ in 0..rc {
                    if let Some(row) = decode_row(&buf, &mut off) {
                        let key = make_join_key(&row, &probe_keys);
                        let pi  = hash_str(&key) % n_parts;
                        enc.clear();
                        encode_row(&row, &mut enc);
                        let enc_snap = enc.clone();
                        pw[pi].push_encoded(&enc_snap, |abs, data| {
                            self.write_block_raw(abs, data)
                        })?;
                    }
                }
            }
        }
        // Flush partial blocks for all probe writers.
        for i in 0..n_parts {
            if pw[i].buf_cnt > 0 {
                let abs      = pw[i].start_block + pw[i].num_blocks;
                let buf_cnt  = pw[i].buf_cnt;
                let buf_len  = pw[i].buf.len();
                pw[i].buf[buf_len - 2] = (buf_cnt & 0xFF) as u8;
                pw[i].buf[buf_len - 1] = (buf_cnt >> 8)   as u8;
                let data = pw[i].buf.clone();
                self.write_block_raw(abs, &data)?;
                pw[i].num_blocks += 1;
                pw[i].buf_cnt = 0;
            }
        }
        let probe_parts: Vec<(u64, u64, usize)> = pw.into_iter()
            .map(|w| (w.start_block, w.num_blocks, w.row_count))
            .collect();
        // pw dropped here → 64 KB freed.

        // ── Per-partition join: one HashMap in memory at a time ───────────────
        let mut out_buf:  Vec<Row>        = Vec::new();
        let mut out_runs: Vec<ScratchRun> = Vec::new();
        let mut out_cap   = 0usize;
        let mut out_total = 0usize;

        for i in 0..n_parts {
            let (b_start, b_nblk, b_rc) = build_parts[i];
            let (p_start, p_nblk, p_rc) = probe_parts[i];
            if b_rc == 0 || p_rc == 0 { continue; }

            // Load build partition into map.
            let mut part_map: HashMap<String, Vec<Row>> =
                HashMap::with_capacity(b_rc.min(4096));
            for bi in 0..b_nblk {
                let buf = self.read_block(b_start + bi)?;
                let rc  = u16::from_le_bytes([buf[bs - 2], buf[bs - 1]]) as usize;
                let mut off = 0usize;
                for _ in 0..rc {
                    if let Some(row) = decode_row(&buf, &mut off) {
                        let key = make_join_key(&row, &build_keys);
                        part_map.entry(key).or_default().push(row);
                    }
                }
            }

            // Stream probe partition.
            for bi in 0..p_nblk {
                let buf = self.read_block(p_start + bi)?;
                let rc  = u16::from_le_bytes([buf[bs - 2], buf[bs - 1]]) as usize;
                let mut off = 0usize;
                for _ in 0..rc {
                    if let Some(p_row) = decode_row(&buf, &mut off) {
                        let key = make_join_key(&p_row, &probe_keys);
                        if let Some(b_rows) = part_map.get(&key) {
                            for b_row in b_rows {
                                let mut row = p_row.clone();
                                row.extend_from_slice(b_row);
                                if !eval_preds(&row, &extra_preds) { continue; }
                                self.emit_row(
                                    row, &mut out_buf, &mut out_runs, &mut out_cap, &mut out_total,
                                )?;
                            }
                        }
                    }
                }
            }
            // part_map dropped; memory reclaimed before next partition.
        }

        let rs = self.finalize_output(out_buf, out_runs, out_total)?;
        eprintln!("[GRACE] out={}", out_total);
        Ok(rs)
    }

    // ── SORT (external merge sort) ────────────────────────────────────────────

    fn exec_sort(&mut self, data: &OptSortData) -> Result<RunSet> {
        let input = self.exec_node(&data.underlying)?;
        let n     = input.row_count();
        eprintln!("[SORT] input={}", n);

        if n == 0 { return Ok(RunSet::InMemory(Vec::new())); }

        let fits_ram = match &input {
            RunSet::InMemory(t) => rough_bytes(t) <= self.memory_limit_bytes / 4,
            RunSet::Spilled { .. } => false,
        };
        if fits_ram {
            let mut t = self.collect_runset(input)?;
            sort_slice(&mut t, &data.sort_specs);
            eprintln!("[SORT] in-place rows={}", t.len());
            return Ok(RunSet::InMemory(t));
        }

        let bpr: usize = match &input {
            RunSet::InMemory(t) if !t.is_empty() => rough_bytes(t) / t.len(),
            _ => 600,
        };
        let pass1_cap = {
            let budget = self.memory_limit_bytes / 4;
            (budget / bpr.max(1)).max(64).min(2048)
        };
        eprintln!("[SORT] external bpr~{} pass1_cap={}", bpr, pass1_cap);

        let specs = data.sort_specs.clone();
        let mut sorted_runs: Vec<ScratchRun> = Vec::new();
        let mut chunk: Vec<Row> = Vec::with_capacity(pass1_cap);

        let input_descs = self.ensure_on_disk(input)?;
        let bs = self.block_size as usize;

        for (start, n_blk) in input_descs {
            for bi in 0..n_blk {
                let buf = self.read_block(start + bi)?;
                let rc  = u16::from_le_bytes([buf[bs - 2], buf[bs - 1]]) as usize;
                let mut off = 0usize;
                for _ in 0..rc {
                    if let Some(row) = decode_row(&buf, &mut off) {
                        chunk.push(row);
                        if chunk.len() >= pass1_cap {
                            sort_slice(&mut chunk, &specs);
                            sorted_runs.push(self.spill(&chunk)?);
                            chunk.clear();
                        }
                    }
                }
            }
        }
        if !chunk.is_empty() {
            sort_slice(&mut chunk, &specs);
            sorted_runs.push(self.spill(&chunk)?);
            chunk.clear();
        }
        drop(chunk);
        eprintln!("[SORT] pass1: {} sorted runs", sorted_runs.len());

        if sorted_runs.len() == 1 {
            let r = sorted_runs.remove(0);
            return Ok(RunSet::Spilled { row_count: r.row_count, runs: vec![r] });
        }

        let mut heads: Vec<MergeHead> = Vec::with_capacity(sorted_runs.len());
        for run in &sorted_runs {
            if run.num_blocks == 0 || run.row_count == 0 { continue; }
            let buf = self.read_block(run.start_block)?;
            let brc = u16::from_le_bytes([buf[bs - 2], buf[bs - 1]]) as usize;
            let mut h = MergeHead {
                run_start:       run.start_block,
                run_blocks:      run.num_blocks,
                cur_block:       0,
                block_buf:       buf,
                block_row_count: brc,
                block_row_idx:   0,
                block_off:       0,
                current_row:     None,
            };
            h.advance(bs);
            if h.current_row.is_some() { heads.push(h); }
        }

        let merge_out_cap =
            (self.memory_limit_bytes / 16 / bpr.max(1)).max(64).min(512);
        eprintln!("[SORT] merge_out_cap={} heads={}", merge_out_cap, heads.len());

        let mut out_buf:  Vec<Row>        = Vec::with_capacity(merge_out_cap);
        let mut out_runs: Vec<ScratchRun> = Vec::new();
        let mut out_total = 0usize;

        loop {
            if heads.is_empty() { break; }

            let mut best = 0usize;
            for i in 1..heads.len() {
                if cmp_rows(
                    heads[i].current_row.as_ref().unwrap(),
                    heads[best].current_row.as_ref().unwrap(),
                    &specs,
                ) == std::cmp::Ordering::Less
                {
                    best = i;
                }
            }

            let row = heads[best].current_row.take().unwrap();
            out_total += 1;
            out_buf.push(row);
            if out_buf.len() >= merge_out_cap {
                let run = self.spill(&out_buf)?;
                out_runs.push(run);
                out_buf.clear();
            }

            heads[best].advance(bs);
            if heads[best].current_row.is_none() {
                let next = heads[best].cur_block + 1;
                if next >= heads[best].run_blocks {
                    heads.swap_remove(best);
                } else {
                    let abs = heads[best].run_start + next;
                    let buf = self.read_block(abs)?;
                    let brc = u16::from_le_bytes([buf[bs - 2], buf[bs - 1]]) as usize;
                    heads[best].cur_block       = next;
                    heads[best].block_buf       = buf;
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

        let rs = self.finalize_output(out_buf, out_runs, out_total)?;
        eprintln!("[SORT] merge done rows={}", rs.row_count());
        Ok(rs)
    }

    pub fn read_block_pub(&mut self, abs: u64) -> Result<Vec<u8>> {
        self.read_block(abs)
    }
}

// ── Free functions ────────────────────────────────────────────────────────────

fn make_join_key(row: &Row, keys: &[String]) -> String {
    let mut out = String::new();
    for (i, k) in keys.iter().enumerate() {
        if i > 0 { out.push('\x00'); }
        let v = row
            .iter()
            .find(|(col, _)| col == k || col.ends_with(&format!(".{}", k)))
            .map(|(_, v)| v.as_str())
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
        .find(|(k, _)| k == name || k.ends_with(&format!(".{}", name)))
        .map(|(_, v)| v)
}

fn project_row(row: &Row, map: &[(String, String)]) -> Row {
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
            None => {
                eprintln!("[PRED] missing col '{}'", p.column_name);
                return false;
            }
        };
        let rhs: Option<String> = match &p.value {
            ComparisionValue::Column(c) => find_col(row, c).cloned(),
            ComparisionValue::String(s) => Some(s.clone()),
            ComparisionValue::I32(v)    => Some(v.to_string()),
            ComparisionValue::I64(v)    => Some(v.to_string()),
            ComparisionValue::F32(v)    => Some(v.to_string()),
            ComparisionValue::F64(v)    => Some(v.to_string()),
        };
        let rhs = match rhs {
            Some(ref r) => r,
            None        => return false,
        };
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
        let va = a.iter()
            .find(|(k, _)| k == &s.column_name || k.ends_with(&format!(".{}", s.column_name)))
            .map(|(_, v)| v);
        let vb = b.iter()
            .find(|(k, _)| k == &s.column_name || k.ends_with(&format!(".{}", s.column_name)))
            .map(|(_, v)| v);
        let ord = match (va, vb) {
            (Some(a), Some(b)) => match (a.parse::<f64>(), b.parse::<f64>()) {
                (Ok(x), Ok(y)) => x.partial_cmp(&y).unwrap_or(Ordering::Equal),
                _              => a.cmp(b),
            },
            (None, None) => Ordering::Equal,
            (None, _)    => Ordering::Less,
            (_, None)    => Ordering::Greater,
        };
        if ord != Ordering::Equal {
            return if s.ascending { ord } else { ord.reverse() };
        }
    }
    Ordering::Equal
}

fn fmt_f64(v: f64) -> String {
    if v.fract() == 0.0 && v.abs() < 1e15 {
        format!("{:.1}", v)
    } else {
        v.to_string()
    }
}

fn rough_bytes(t: &Table) -> usize {
    let p = std::mem::size_of::<usize>();
    3 * p
        + t.iter()
            .map(|r| {
                3 * p + r.iter().map(|(k, v)| 6 * p + k.len() + v.len()).sum::<usize>()
            })
            .sum::<usize>()
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

pub fn get_output_columns(op: &OptQueryOp, ctx: &DbContext) -> Vec<String> {
    match op {
        OptQueryOp::Scan(d) => ctx
            .get_table_specs()
            .iter()
            .find(|t| t.name == d.table_id)
            .map(|s| {
                s.column_specs
                    .iter()
                    .map(|c| format!("{}.{}", d.table_id, c.column_name))
                    .collect()
            })
            .unwrap_or_default(),
        OptQueryOp::Filter(d)  => get_output_columns(&d.underlying, ctx),
        OptQueryOp::Sort(d)    => get_output_columns(&d.underlying, ctx),
        OptQueryOp::Project(d) => d.column_name_map.iter().map(|(_, to)| to.clone()).collect(),
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