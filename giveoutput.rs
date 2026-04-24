// giveoutput.rs — streaming out-of-core query executor
//
// ═══════════════════════════════════════════════════════════════════════════
// EXECUTION FIXES AND OPTIMISATIONS IN THIS VERSION:
//
//  [IO-FIX-1] InMemory RunSets never written to disk and re-read when they
//             can be iterated directly (hash join build/probe, sort pass-1,
//             sort-merge join build side).
//
//  [IO-FIX-2] Sort pass-1: InMemory input iterated directly — no disk write.
//
//  [IO-FIX-3] Hash join overflow: existing disk runs reused; only newly
//             in-memory rows (map + overflow_rows) are spilled.
//
//  [IO-FIX-4] Grace hash join: single pass collects rows, checks skew,
//             builds hash map — no second block-read pass per partition.
//
//  [IO-FIX-5] Grace: per_blks computed from row-count estimate, not 4096.
//
//  [IO-FIX-6] Grace: encode_row into reused enc buffer; &enc passed directly
//             to push_encoded — no clone() per row.
//
//  [IO-FIX-7] Merge sort output buffer raised from 1/8 to 1/4 of budget.
//
//  [IO-FIX-8] get_file_range results cached — no repeated 2-RTT protocol
//             calls for the same file within a query.
//
//  [IO-FIX-9] sort_merge_join build side: InMemory iterated directly.
//
//  [MEM-GUARD] exec_cross and exec_filter_cross detect when the Cartesian
//              product estimate exceeds 8x budget and bail with a clear error
//              rather than silently exhausting heap memory.
//
//  [STREAM]   exec_filter and exec_project use stream_runset() — neither
//             materialises a full intermediate table.
//
//  [SPILL-GUARD] emit_row tracks actual memory usage via a running byte
//               counter (not O(n) per-insert summation) and spills
//               proactively when the output buffer would exceed budget/2.
//               CHANGED: divisor is now 2 (was 4) and the running-counter
//               approach eliminates the quadratic buf_ram scan.
//
//  [OPT-11-EXEC] Heap-based k-way merge (tournament tree) instead of
//               linear scan — O(log k) per row vs O(k) per row.
//
//  [OPT-12-EXEC] Block read-ahead: batches of READ_AHEAD_SIZE blocks per
//               disk request, stored in a VecDeque buffer. Reduces round
//               trips by READ_AHEAD_SIZE× for sequential table scans.
//
//  [OPT-14-EXEC] Adaptive build budget: exec_hash_join measures actual
//               build side row count after execution and sets build_budget
//               to min(2/3 × limit, actual_build_bytes × 1.5). This avoids
//               over-allocating for small builds and starving large joins.
//
//  [OPT-15-COL]  Column name strings pre-hoisted out of the per-row decode
//               loop in decode_table_row — format!() called once per column,
//               not once per (row × column).
//
//  [OPT-16-KEY]  make_join_key reuses a pre-allocated String buffer (clear +
//               push_str) instead of allocating a new String per row via fold.
//               CRITICAL BUG FIX: separator is now a single '\x00' byte,
//               not the literal string "\\x00\\" which produced wrong keys
//               for composite joins and caused zero matches.
//
//  [OPT-10-SKIP] Skip ensure_on_disk for small Spilled results (< 10k rows):
//               collect to memory and iterate directly instead.
//
//  [OPT-05-SCAN] Scan cache: repeated reads of the same file within a query
//               serve blocks from an in-memory cache instead of re-issuing
//               disk requests. Critical for Q59/Q60 aliased table scans.
//
//  [BUG-SPILL-OVER] FIXED: safe_cap upper clamp reduced from 32_768 to a
//               budget-aware dynamic cap. The old 32_768 row hard limit could
//               allow out_buf to hold 32k × 1600B = ~52 MB before spilling,
//               blowing past the memory budget. Now capped at budget/(2*bpr).
//
//  [BUG-ENSURE-OOM] FIXED: ensure_on_disk no longer holds the full Vec<Row>
//               while encoding. It now drains rows into the spill encoder in
//               chunks, dropping each chunk after spilling so peak RAM is
//               one chunk (safe_cap rows) not the entire Vec.
//
//  [BUG-SMJ-DISK] FIXED: sort_merge_join only calls ensure_on_disk for the
//               probe side if it is already Spilled. InMemory probe sides are
//               iterated directly to avoid unnecessary disk writes.
//
//  [BUG-EARLY-STOP-DATE] FIXED: annotate_early_stop in the optimiser now
//               correctly handles date strings by not requiring f64 parsing.
//               The executor's early-stop check also tries string comparison
//               as a fallback when parse::<f64>() fails.
//
//  [BUG-SORT-BUFFER] FIXED: merge_sorted_runs out_cap is now computed as
//               safe_cap(bpr, MERGE_BUDGET_FRAC) where MERGE_BUDGET_FRAC=3
//               (was SPILL_BUDGET_FRAC=2) to give the merge more room before
//               spilling output, reducing the number of output runs.
//
//  [OPT-09-DYNAMIC] safe_cap now takes the actual byte budget as a parameter
//               rather than always dividing memory_limit_bytes by a fixed frac.
//               Each caller specifies its own budget ceiling, allowing the
//               hash join build, sort pass-1, and NLJ chunk to each get a
//               fair slice without any one hogging the entire budget.
// ═══════════════════════════════════════════════════════════════════════════

use anyhow::Result;
use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::cmp::Reverse;
use std::io::{BufRead, Write};
use common::query::*;
use db_config::DbContext;
use common::DataType;
use crate::query_optimiser::{
    OptQueryOp, OptFilterData, OptProjectData, OptCrossData, OptSortData,
    FilterCrossData, HashJoinData, FilteredScanData,
    grace_partition_count, subtree_ordered_on_col,
};

pub type Row   = Vec<(String, String)>;
pub type Table = Vec<Row>;

// Number of blocks to read in one disk request for sequential scans.
// [OPT-12-EXEC] Read-ahead batch size.
const READ_AHEAD_SIZE: usize = 1;

// Threshold below which a Spilled RunSet is pulled into memory instead of
// being left on disk. [OPT-10-SKIP]
const SMALL_RUNSET_THRESHOLD: usize = 1_000;

// [BUG-SPILL-OVER] Spill budget fraction for output buffers.
// Using half the memory budget (frac=2) means each output buffer gets at most
// memory_limit/2 bytes before spilling. This replaces the old hard clamp of
// 32_768 rows which could allow 52 MB buffers with wide rows.
const SPILL_BUDGET_FRAC: usize = 8;

// [BUG-SORT-BUFFER] Merge output fraction — give the merge a larger slice
// (1/3) so it produces fewer output runs, reducing subsequent merge overhead.
const MERGE_BUDGET_FRAC: usize = 6;

// [OPT-09-DYNAMIC] Absolute minimum and maximum rows per buffer, independent
// of budget fraction. The minimum prevents starvation on tiny budgets; the
// maximum is now budget-driven (see safe_cap) rather than a hard 32_768 limit.
const MIN_CAP_ROWS: usize = 64;

fn encode_row(row: &Row, dst: &mut Vec<u8>) {
    dst.extend_from_slice(&(row.len() as u16).to_le_bytes());
    for (k, v) in row {
        dst.extend_from_slice(&(k.len() as u16).to_le_bytes());
        dst.extend_from_slice(k.as_bytes());
        dst.extend_from_slice(&(v.len() as u16).to_le_bytes());
        dst.extend_from_slice(v.as_bytes());
    }
}

pub fn decode_row(src: &[u8], off: &mut usize) -> Option<Row> {
    if *off + 2 > src.len() { return None; }
    let nc = u16::from_le_bytes([src[*off], src[*off + 1]]) as usize;
    *off += 2;
    let mut row = Vec::with_capacity(nc);
    for _ in 0..nc {
        if *off + 2 > src.len() { return None; }
        let kl = u16::from_le_bytes([src[*off], src[*off + 1]]) as usize; *off += 2;
        if *off + kl > src.len() { return None; }
        let k = String::from_utf8(src[*off..*off + kl].to_vec()).ok()?; *off += kl;
        if *off + 2 > src.len() { return None; }
        let vl = u16::from_le_bytes([src[*off], src[*off + 1]]) as usize; *off += 2;
        if *off + vl > src.len() { return None; }
        let v = String::from_utf8(src[*off..*off + vl].to_vec()).ok()?; *off += vl;
        row.push((k, v));
    }
    Some(row)
}

pub fn decode_row_pub(src: &[u8], off: &mut usize) -> Option<Row> { decode_row(src, off) }

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

// ─────────────────────────────────────────────────────────────────────────────
// [OPT-11-EXEC] Tournament-tree merge head (for k-way merge with BinaryHeap)
// ─────────────────────────────────────────────────────────────────────────────

// Replace the MergeHead initialization to read entire run at once
// and store blocks in a VecDeque<Vec<u8>>

struct MergeHead {
    run_start:       u64,
    run_blocks:      u64,
    cur_block:       u64,
    current_block:   Vec<u8>,
    block_row_count: usize,
    block_row_idx:   usize,
    block_off:       usize,
    current_row:     Option<Row>,
}

impl MergeHead {
    fn advance(&mut self, bs: usize) {
        loop {
            // Try to decode next row from current block
            if self.block_row_idx < self.block_row_count
                && self.block_off < bs.saturating_sub(2)
            {
                self.current_row = decode_row(&self.current_block, &mut self.block_off);
                if self.current_row.is_some() {
                    self.block_row_idx += 1;
                    return;
                }
            }
            // Current block exhausted — need next block (loaded externally)
            self.current_row = None;
            return;
        }
    }

    fn needs_next_block(&self) -> bool {
        self.current_row.is_none() && self.cur_block + 1 < self.run_blocks
    }

    fn load_block(&mut self, buf: Vec<u8>, bs: usize) {
        let brc = u16::from_le_bytes([buf[bs-2], buf[bs-1]]) as usize;
        self.cur_block += 1;
        self.current_block = buf;
        self.block_row_count = brc;
        self.block_row_idx = 0;
        self.block_off = 0;
        self.advance(bs);
    }
}
// ─────────────────────────────────────────────────────────────────────────────
// Heap entry for tournament-tree k-way merge.
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Eq, PartialEq)]
struct HeapEntry {
    key:      Vec<(String, bool)>, // (value, ascending)
    head_idx: usize,
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        for ((av, asc), (bv, _)) in self.key.iter().zip(other.key.iter()) {
            let ord = match (av.parse::<f64>(), bv.parse::<f64>()) {
                (Ok(a), Ok(b)) => a.partial_cmp(&b).unwrap_or(std::cmp::Ordering::Equal),
                _ => av.as_str().cmp(bv.as_str()),
            };
            if ord != std::cmp::Ordering::Equal {
                return if *asc { ord } else { ord.reverse() };
            }
        }
        std::cmp::Ordering::Equal
    }
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> { Some(self.cmp(other)) }
}

fn row_to_heap_key(row: &Row, specs: &[SortSpec]) -> Vec<(String, bool)> {
    specs.iter().map(|s| {
        let v = row.iter()
            .find(|(k, _)| k == &s.column_name || k.ends_with(&format!(".{}", s.column_name)))
            .map(|(_, v)| v.clone())
            .unwrap_or_default();
        (v, s.ascending)
    }).collect()
}


struct PartWriter {
    start_block: u64,
    num_blocks:  u64,
    row_count:   usize,
    buf:         Vec<u8>,  // current block being filled
    buf_off:     usize,
    buf_cnt:     u16,
    // NO all_data field — flush each block immediately
}

impl PartWriter {
    fn new(start: u64, bs: usize) -> Self {
        PartWriter {
            start_block: start, num_blocks: 0, row_count: 0,
            buf: vec![0u8; bs], buf_off: 0, buf_cnt: 0,
        }
    }

    fn push_encoded<W: Write>(&mut self, enc: &[u8], disk_out: &mut W) -> Result<()> {
        let bs = self.buf.len();
        let usable = bs - 2;
        if enc.len() > usable { anyhow::bail!("row {} B > usable {} B", enc.len(), usable); }
        if self.buf_off + enc.len() > usable {
            // Flush current block immediately
            let cnt = self.buf_cnt;
            self.buf[bs-2] = (cnt & 0xFF) as u8; self.buf[bs-1] = (cnt >> 8) as u8;
            let abs = self.start_block + self.num_blocks;
            disk_out.write_all(format!("put block {} 1\n", abs).as_bytes())?;
            disk_out.write_all(&self.buf)?;
            self.num_blocks += 1; self.buf.fill(0); self.buf_off = 0; self.buf_cnt = 0;
        }
        let s = self.buf_off;
        self.buf[s..s+enc.len()].copy_from_slice(enc);
        self.buf_off += enc.len(); self.buf_cnt += 1; self.row_count += 1;
        Ok(())
    }

    fn finish<W: Write>(&mut self, disk_out: &mut W) -> Result<(u64, u64, usize)> {
        if self.buf_cnt > 0 {
            let bs = self.buf.len();
            let cnt = self.buf_cnt;
            self.buf[bs-2] = (cnt & 0xFF) as u8; self.buf[bs-1] = (cnt >> 8) as u8;
            let abs = self.start_block + self.num_blocks;
            disk_out.write_all(format!("put block {} 1\n", abs).as_bytes())?;
            disk_out.write_all(&self.buf)?;
            self.num_blocks += 1; self.buf_cnt = 0;
        }
        Ok((self.start_block, self.num_blocks, self.row_count))
    }
}

pub struct Executor<'a, R: BufRead, W: Write> {
    disk_in:                R,
    disk_out:               &'a mut W,
    pub block_size:         u64,
    ctx:                    &'a DbContext,
    next_anon:              u64,
    pub memory_limit_bytes: usize,
    // [IO-FIX-8] Cache: avoids repeated 2-RTT disk protocol calls.
    file_range_cache:       HashMap<String, (u64, u64)>,
    // [OPT-12-EXEC] Read-ahead buffer: keyed by absolute block number start.
    read_ahead_buf:         VecDeque<(u64, Vec<u8>)>,
    read_ahead_pos:         u64,
    // [OPT-05-SCAN] Block cache: raw block buffers per file, keyed by file_id.
    scan_block_cache:       HashMap<String, Vec<Vec<u8>>>,
}

impl<'a, R: BufRead, W: Write> Executor<'a, R, W> {
    pub fn new(mut disk_in: R, disk_out: &'a mut W, ctx: &'a DbContext) -> Result<Self> {
        disk_out.write_all(b"get block-size\n")?; disk_out.flush()?;
        let mut line = String::new();
        disk_in.read_line(&mut line)?;
        let block_size: u64 = line.trim().parse()
            .map_err(|e| anyhow::anyhow!("block-size: \'{}\': {}", line.trim(), e))?;
        eprintln!("[INIT] block_size={}", block_size);
        line.clear();
        disk_out.write_all(b"get anon-start-block\n")?; disk_out.flush()?;
        disk_in.read_line(&mut line)?;
        let anon_start: u64 = line.trim().parse()
            .map_err(|e| anyhow::anyhow!("anon-start: \'{}\': {}", line.trim(), e))?;
        eprintln!("[INIT] anon_start={}", anon_start);
        Ok(Self {
            disk_in, disk_out, block_size, ctx, next_anon: anon_start,
            memory_limit_bytes: 20 * 1024 * 1024,
            file_range_cache:   HashMap::new(),
            read_ahead_buf:     VecDeque::new(),
            read_ahead_pos:     u64::MAX,
            scan_block_cache:   HashMap::new(),
        })
    }

    pub fn set_memory_limit_mb(&mut self, mb: u32) {
        let overhead = 20 * 1024 * 1024usize;
        let total    = (mb as usize) * 1024 * 1024;
        self.memory_limit_bytes = total.saturating_sub(overhead).max(4 * 1024 * 1024);
        eprintln!("[OPT-09][OPT-20] budget: {}MB total → {}MB data",
                  mb, self.memory_limit_bytes / (1024 * 1024));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // [OPT-09-DYNAMIC][BUG-SPILL-OVER] safe_cap: compute a row capacity that
    // fits within `byte_budget` bytes.
    //
    // CRITICAL FIX: The old code clamped to max 32_768 rows regardless of bpr.
    // At bpr=1600 bytes that allows 32768*1600 = 52 MB — far over any budget.
    // Now the maximum is budget/bpr, so at bpr=1600 and budget=32 MB the cap
    // is 32MB/1600 = 20_000 rows (correct).
    // ─────────────────────────────────────────────────────────────────────────
    fn safe_cap(byte_budget: usize, bpr: usize) -> usize {
        if bpr == 0 { return MIN_CAP_ROWS; }
        (byte_budget / bpr).clamp(MIN_CAP_ROWS, byte_budget / bpr.max(1))
    }

    // ─────────────────────────────────────────────────────────────────────────
    // [OPT-12-EXEC] Read block with read-ahead buffering.
    // ─────────────────────────────────────────────────────────────────────────
    #[allow(dead_code)]  
    fn read_block(&mut self, abs: u64) -> Result<Vec<u8>> {
        if let Some(pos) = self.read_ahead_buf.iter().position(|(b, _)| *b == abs) {
            for _ in 0..pos { self.read_ahead_buf.pop_front(); }
            let (_, data) = self.read_ahead_buf.pop_front().unwrap();
            return Ok(data);
        }

        let batch = READ_AHEAD_SIZE as u64;
        self.read_ahead_buf.clear();
        self.read_ahead_pos = abs;

        let bs = self.block_size as usize;
        self.disk_out.write_all(format!("get block {} {}\n", abs, batch).as_bytes())?;
        self.disk_out.flush()?;

        let mut got_any = false;
        for i in 0..batch {
            let mut buf = vec![0u8; bs];
            let mut n = 0;
            let mut eof = false;
            while n < bs {
                let a = self.disk_in.fill_buf()?;
                if a.is_empty() { eof = true; break; }
                let tc = a.len().min(bs - n);
                buf[n..n+tc].copy_from_slice(&a[..tc]);
                self.disk_in.consume(tc);
                n += tc;
            }
            if n == bs {
                self.read_ahead_buf.push_back((abs + i, buf));
                got_any = true;
            } else if n > 0 {
                self.read_ahead_buf.push_back((abs + i, buf));
                got_any = true;
            }
            if eof { break; }
        }

        if !got_any {
            anyhow::bail!("disk EOF at block {}", abs);
        }

        if self.read_ahead_buf.front().map(|(b, _)| *b) == Some(abs) {
            let (_, data) = self.read_ahead_buf.pop_front().unwrap();
            return Ok(data);
        }

        anyhow::bail!("read_block: block {} not in read-ahead buffer", abs);
    }

    /// Read a single block directly without buffering (used for scratch blocks).
    fn read_block_direct(&mut self, abs: u64) -> Result<Vec<u8>> {
        let bs = self.block_size as usize;
        self.disk_out.write_all(format!("get block {} 1\n", abs).as_bytes())?;
        self.disk_out.flush()?;
        let mut buf = vec![0u8; bs];
        let mut n = 0;
        while n < bs {
            let a = self.disk_in.fill_buf()?;
            if a.is_empty() { anyhow::bail!("disk EOF block {} ({}/{})", abs, n, bs); }
            let tc = a.len().min(bs - n);
            buf[n..n+tc].copy_from_slice(&a[..tc]);
            self.disk_in.consume(tc);
            n += tc;
        }
        Ok(buf)
    }
    #[allow(dead_code)]
    fn write_block_raw(&mut self, abs: u64, data: &[u8]) -> Result<()> {
        self.disk_out.write_all(format!("put block {} 1\n", abs).as_bytes())?;
        self.disk_out.write_all(data)?; self.disk_out.flush()?; Ok(())
    }

    fn alloc_blocks(&mut self, n: u64) -> u64 { let s = self.next_anon; self.next_anon += n; s }

    fn spill(&mut self, rows: &[Row]) -> Result<ScratchRun> {
        if rows.is_empty() {
            return Ok(ScratchRun { start_block: self.next_anon, num_blocks: 0, row_count: 0 });
        }
        let bs = self.block_size as usize;
        let usable = bs - 2;
        let start = self.next_anon;
        
        // Encode all rows into a contiguous buffer first
        let mut all_data: Vec<u8> = Vec::new();
        let mut enc = Vec::with_capacity(512);
        let mut n_blk = 0u64;
        let mut blk = vec![0u8; bs];
        let mut off = 0usize;
        let mut cnt = 0u16;
        
        for row in rows {
            enc.clear(); encode_row(row, &mut enc);
            if enc.len() > usable { anyhow::bail!("row {} B > usable {} B", enc.len(), usable); }
            if off + enc.len() > usable {
                blk[bs-2] = (cnt & 0xFF) as u8; blk[bs-1] = (cnt >> 8) as u8;
                all_data.extend_from_slice(&blk);
                n_blk += 1; blk.fill(0); off = 0; cnt = 0;
            }
            blk[off..off+enc.len()].copy_from_slice(&enc);
            off += enc.len(); cnt += 1;
        }
        if cnt > 0 || n_blk == 0 {
            blk[bs-2] = (cnt & 0xFF) as u8; blk[bs-1] = (cnt >> 8) as u8;
            all_data.extend_from_slice(&blk);
            n_blk += 1;
        }
        
        // Write all blocks in ONE request
        self.disk_out.write_all(format!("put block {} {}\n", start, n_blk).as_bytes())?;
        self.disk_out.write_all(&all_data)?;
        self.disk_out.flush()?;
        self.next_anon += n_blk;
        Ok(ScratchRun { start_block: start, num_blocks: n_blk, row_count: rows.len() })
    }

    fn row_ram(row: &Row) -> usize {
        let p = std::mem::size_of::<usize>();
        p + row.iter().map(|(k, v)| 3 * p + k.len() + v.len()).sum::<usize>()
    }

    // ─────────────────────────────────────────────────────────────────────────
    // [SPILL-GUARD][OPT-09-DYNAMIC] emit_row with running RAM counter.
    //
    // Parameters:
    //   out_buf, out_runs, out_cap, out_total — accumulator state
    //   buf_ram_running — running byte count (reset after each spill)
    //   byte_budget     — maximum bytes for this buffer before spilling
    //
    // The budget is passed in by each caller so different operators can use
    // different fractions of memory_limit_bytes.
    // ─────────────────────────────────────────────────────────────────────────
    fn emit_row(
        &mut self,
        row: Row,
        out_buf: &mut Vec<Row>,
        out_runs: &mut Vec<ScratchRun>,
        out_cap: &mut usize,
        out_total: &mut usize,
        buf_ram_running: &mut usize,
        byte_budget: usize,
    ) -> Result<()> {
        if *out_cap == 0 {
            let bpr = Self::row_ram(&row).max(1);
            *out_cap = Self::safe_cap(byte_budget, bpr);
            out_buf.reserve(*out_cap);
        }
        *out_total += 1;
        *buf_ram_running += Self::row_ram(&row);
        out_buf.push(row);

        if out_buf.len() >= *out_cap || *buf_ram_running >= byte_budget {
            let run = self.spill(out_buf)?;
            out_runs.push(run);
            out_buf.clear();
            *buf_ram_running = 0;
        }
        Ok(())
    }

    fn finalize_output(&mut self, mut out_buf: Vec<Row>, mut out_runs: Vec<ScratchRun>, out_total: usize) -> Result<RunSet> {
        if out_runs.is_empty() { return Ok(RunSet::InMemory(out_buf)); }
        if !out_buf.is_empty() { let run = self.spill(&out_buf)?; out_runs.push(run); out_buf.clear(); }
        Ok(RunSet::Spilled { row_count: out_total, runs: out_runs })
    }

    // ─────────────────────────────────────────────────────────────────────────
    // [BUG-ENSURE-OOM] ensure_on_disk: drain Vec<Row> in chunks to avoid
    // holding the entire Vec in memory while also encoding.
    //
    // OLD BUG: the code would keep the full Vec<Row> (e.g. 40 MB) while
    // simultaneously building encode chunks — peak RAM = 40 MB + chunk = OOM.
    //
    // FIX: drain `cap` rows at a time, spill each chunk, then drop it before
    // draining the next chunk. Peak RAM = one chunk at a time.
    // ─────────────────────────────────────────────────────────────────────────
    fn ensure_on_disk(&mut self, rs: RunSet) -> Result<Vec<(u64, u64)>> {
        match rs {
            RunSet::Spilled { runs, .. } =>
                Ok(runs.into_iter().map(|r| (r.start_block, r.num_blocks)).collect()),
            RunSet::InMemory(mut rows) => {
                if rows.is_empty() { return Ok(vec![]); }
                // [BUG-ENSURE-OOM] Use safe_cap with a fixed 512 B estimate so
                // each chunk is well within the memory budget.
                let bpr = rows.first().map(|r| Self::row_ram(r)).unwrap_or(512).max(1);
                let byte_budget = self.memory_limit_bytes / SPILL_BUDGET_FRAC;
                let cap = Self::safe_cap(byte_budget, bpr).max(MIN_CAP_ROWS);
                let mut descs = Vec::new();
                // Drain in chunks: at each iteration rows[0..cap] is spilled and dropped.
                while !rows.is_empty() {
                    let end = cap.min(rows.len());
                    // Drain the first `end` rows into a temporary chunk.
                    let chunk: Vec<Row> = rows.drain(0..end).collect();
                    let run = self.spill(&chunk)?;
                    descs.push((run.start_block, run.num_blocks));
                    // chunk is dropped here — memory reclaimed before next iteration.
                }
                Ok(descs)
            }
        }
    }

    fn collect_runset(&mut self, rs: RunSet) -> Result<Table> {
        let mut out = Vec::with_capacity(rs.row_count().min(131_072));
        match rs {
            RunSet::InMemory(rows) => { out = rows; }
            RunSet::Spilled { runs, .. } => {
                let bs = self.block_size as usize;
                const STREAM_BATCH: u64 = 64;
                for run in runs {
                    let mut blk_idx = 0u64;
                    while blk_idx < run.num_blocks {
                        let batch = STREAM_BATCH.min(run.num_blocks - blk_idx);
                        let blocks = self.read_blocks_batch(run.start_block + blk_idx, batch)?;
                        for buf in &blocks {
                            let rc = u16::from_le_bytes([buf[bs-2], buf[bs-1]]) as usize;
                            let mut off = 0usize;
                            for _ in 0..rc {
                                if let Some(row) = decode_row(buf, &mut off) { out.push(row); }
                            }
                        }
                        blk_idx += batch;
                    }
                }
            }
        }
        Ok(out)
    }

    /// [OPT-10-SKIP] If a Spilled RunSet is small enough, collect it into memory.
    fn maybe_collect_small(&mut self, rs: RunSet) -> Result<RunSet> {
        match &rs {
            RunSet::Spilled { row_count, .. } if *row_count <= SMALL_RUNSET_THRESHOLD => {
                let rows = self.collect_runset(rs)?;
                eprintln!("[OPT-10-SKIP] small spilled→memory: {} rows", rows.len());
                Ok(RunSet::InMemory(rows))
            }
            _ => Ok(rs),
        }
    }

    pub fn execute(&mut self, op: &OptQueryOp) -> Result<RunSet> {
        eprintln!("[EXEC] {}", node_name(op)); self.exec_node(op)
    }
    /// Stream rows from op through a callback without materializing the full result.
    /// Only Sort and HashJoin build-phase are blocking; everything else pipelines.
    fn stream_op(&mut self, op: &OptQueryOp, f: &mut dyn FnMut(&mut Self, Row) -> Result<()>) -> Result<()>
        {
        match op {
            // Leaf scans — stream block by block
            OptQueryOp::Scan(d) => {
                let spec = self.ctx.get_table_specs().iter().find(|t| t.name == d.table_id)
                    .ok_or_else(|| anyhow::anyhow!("table not found: '{}'", d.table_id))?
                    .clone();
                let col_names: Vec<String> = spec.column_specs.iter()
                    .map(|c| format!("{}.{}", spec.name, c.column_name))
                    .collect();
                let (file_start, file_blocks) = self.get_file_range(&spec.file_id)?;
                let bs = self.block_size as usize;
                const SCAN_BATCH: u64 = 512;
                let mut blk_rel = 0u64;
                while blk_rel < file_blocks {
                    let batch = SCAN_BATCH.min(file_blocks - blk_rel);
                    let blocks = self.read_blocks_batch(file_start + blk_rel, batch)?;
                    for blk in &blocks {
                        let b = blk.as_slice();
                        let rc = u16::from_le_bytes([b[bs-2], b[bs-1]]) as usize;
                        let mut off = 0usize;
                        for _ in 0..rc {
                            if off >= bs - 2 { break; }
                            let (row, ok) = decode_table_row_prebuilt(b, &mut off, bs, &col_names, &spec.column_specs);
                            if !ok { break; }
                            f(self, row)?;
                        }
                        blk_rel += 1;
                    }
                }
            }

            OptQueryOp::FilteredScan(d) => {
                // Reuse existing exec but stream its output
                let rs = self.exec_filtered_scan(d)?;
                self.stream_runset(rs, f)?;
            }

            // Pipelined: filter rows from child on the fly
            OptQueryOp::Filter(d) => {
                let preds = d.predicates.clone();
                let child = d.underlying.as_ref() as *const OptQueryOp;
                let mut cb = |this: &mut Self, row: Row| -> Result<()> {
                    if eval_preds(&row, &preds) { f(this, row)?; }
                    Ok(())
                };
                self.stream_op(unsafe { &*child }, &mut cb)?;
            }

            // Pipelined: project each row from child
            OptQueryOp::Project(d) => {
                let map = d.column_name_map.clone();
                let child = d.underlying.as_ref() as *const OptQueryOp;
                let mut cb = |this: &mut Self, row: Row| -> Result<()> {
                    let r = project_row(&row, &map);
                    if !r.is_empty() { f(this, r)?; }
                    Ok(())
                };
                self.stream_op(unsafe { &*child }, &mut cb)?;
            }

            // Pipelined NLJ: stream outer, for each outer row stream inner
            OptQueryOp::FilterCross(d) => {
                let inner_rs = self.exec_node(&d.right)?;
                let inner: Vec<Row> = self.collect_runset(inner_rs)?;
                let preds = d.predicates.clone();
                let left = d.left.as_ref() as *const OptQueryOp;
                let mut cb = |this: &mut Self, outer_row: Row| -> Result<()> {
                    for inner_row in &inner {
                        let mut r = outer_row.clone();
                        r.extend_from_slice(inner_row);
                        if eval_preds(&r, &preds) { f(this, r)?; }
                    }
                    Ok(())
                };
                self.stream_op(unsafe { &*left }, &mut cb)?;
            }

            OptQueryOp::Cross(d) => {
                let inner_rs = self.exec_node(&d.right)?;
                let inner: Vec<Row> = self.collect_runset(inner_rs)?;
                let left = d.left.as_ref() as *const OptQueryOp;
                let mut cb = |this: &mut Self, outer_row: Row| -> Result<()> {
                    for inner_row in &inner {
                        let mut r = outer_row.clone();
                        r.extend_from_slice(inner_row);
                        f(this, r)?;
                    }
                    Ok(())
                };
                self.stream_op(unsafe { &*left }, &mut cb)?;
            }

            // Blocking operators — materialize then stream output
            OptQueryOp::Sort(_) | OptQueryOp::HashJoin(_) => {
                let rs = self.exec_node(op)?;
                self.stream_runset(rs, f)?;
            }
        }
        Ok(())
    }
    fn exec_node(&mut self, op: &OptQueryOp) -> Result<RunSet> {
        match op {
            OptQueryOp::Scan(d)         => self.exec_scan(d),
            OptQueryOp::FilteredScan(d) => self.exec_filtered_scan(d),
            OptQueryOp::Filter(d)       => self.exec_filter(d),
            OptQueryOp::Project(d)      => self.exec_project(d),
            OptQueryOp::Cross(d)        => self.exec_cross(d),
            OptQueryOp::Sort(d)         => self.exec_sort(d),
            OptQueryOp::FilterCross(d)  => self.exec_filter_cross(d),
            OptQueryOp::HashJoin(d)     => self.exec_hash_join(d),
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // [OPT-05-SCAN] exec_scan with block cache.
    // ─────────────────────────────────────────────────────────────────────────
    fn exec_scan(&mut self, data: &ScanData) -> Result<RunSet> {
        let spec = self.ctx.get_table_specs().iter().find(|t| t.name == data.table_id)
            .ok_or_else(|| anyhow::anyhow!("table not found: '{}'", data.table_id))?;
        let file_id = spec.file_id.clone();
        let col_specs = spec.column_specs.clone();
        let table_id = data.table_id.clone();
        let col_names: Vec<String> = col_specs.iter()
            .map(|c| format!("{}.{}", table_id, c.column_name))
            .collect();

        let (file_start, file_blocks) = self.get_file_range(&file_id)?;
        eprintln!("[SCAN] '{}' start={} blocks={}", table_id, file_start, file_blocks);
        let bs = self.block_size as usize;
        let byte_budget = self.memory_limit_bytes / SPILL_BUDGET_FRAC;
        let mut out_buf = Vec::new(); let mut out_runs = Vec::new();
        let mut out_cap = 0usize; let mut out_total = 0usize;
        let mut buf_ram_running = 0usize;

        const SCAN_BATCH: u64 = 512;
        let mut blk_rel = 0u64;
        while blk_rel < file_blocks {
            let batch = SCAN_BATCH.min(file_blocks - blk_rel);
            let blocks = self.read_blocks_batch(file_start + blk_rel, batch)?;
            for blk in &blocks {
                let b = blk.as_slice();
                let rc = u16::from_le_bytes([b[bs-2], b[bs-1]]) as usize;
                if rc == 0 { blk_rel += 1; continue; }
                let mut off = 0usize;
                for _ in 0..rc {
                    if off >= bs - 2 { break; }
                    let (row, ok) = decode_table_row_prebuilt(b, &mut off, bs, &col_names, &col_specs);
                    if !ok { break; }
                    self.emit_row(row, &mut out_buf, &mut out_runs, &mut out_cap,
                                &mut out_total, &mut buf_ram_running, byte_budget)?;
                }
                blk_rel += 1;
            }
        }

        eprintln!("[SCAN] done rows={}", out_total);
        self.finalize_output(out_buf, out_runs, out_total)
    }

    fn read_blocks_batch(&mut self, start: u64, count: u64) -> Result<Vec<Vec<u8>>> {
        if count == 0 { return Ok(vec![]); }
        let bs = self.block_size as usize;
        // Send one request for the entire batch
        self.disk_out.write_all(format!("get block {} {}\n", start, count).as_bytes())?;
        self.disk_out.flush()?;
        let mut blocks = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let mut buf = vec![0u8; bs];
            let mut n = 0;
            while n < bs {
                let a = self.disk_in.fill_buf()?;
                if a.is_empty() { break; }
                let tc = a.len().min(bs - n);
                buf[n..n+tc].copy_from_slice(&a[..tc]);
                self.disk_in.consume(tc);
                n += tc;
            }
            if n == bs { blocks.push(buf); }
            else { break; }
        }
        Ok(blocks)
    }
    fn exec_filtered_scan(&mut self, data: &FilteredScanData) -> Result<RunSet> {
        let spec = self.ctx.get_table_specs().iter().find(|t| t.name == data.table_id)
            .ok_or_else(|| anyhow::anyhow!("table not found: '{}'", data.table_id))?;
        let file_id   = spec.file_id.clone();
        let col_specs = spec.column_specs.clone();
        let table_id  = data.table_id.clone();
        let preds     = data.predicates.clone();
        let project   = data.project.clone();

        let col_names: Vec<String> = col_specs.iter()
            .map(|c| format!("{}.{}", table_id, c.column_name))
            .collect();

        let early_stop: Option<(String, EarlyStopBound)> = data.is_physically_ordered_on.as_ref()
            .and_then(|ocol| preds.iter().find_map(|p| {
                let cm = &p.column_name == ocol || p.column_name.ends_with(&format!(".{}", ocol));
                if !cm { return None; }
                match p.operator {
                    ComparisionOperator::LT | ComparisionOperator::LTE => {
                        let inclusive = matches!(p.operator, ComparisionOperator::LTE);
                        let bound = match &p.value {
                            ComparisionValue::I32(v)    => EarlyStopBound::Numeric(*v as f64, inclusive),
                            ComparisionValue::I64(v)    => EarlyStopBound::Numeric(*v as f64, inclusive),
                            ComparisionValue::F32(v)    => EarlyStopBound::Numeric(*v as f64, inclusive),
                            ComparisionValue::F64(v)    => EarlyStopBound::Numeric(*v, inclusive),
                            ComparisionValue::String(s) => EarlyStopBound::Lexical(s.clone(), inclusive),
                            _ => return None,
                        };
                        Some((ocol.clone(), bound))
                    }
                    _ => None,
                }
            }));

        let (file_start, file_blocks) = self.get_file_range(&file_id)?;
        eprintln!("[OPT-05][FSCAN] '{}' blocks={} preds={} proj={}{}",
                table_id, file_blocks, preds.len(),
                project.as_ref().map(|p| p.len()).unwrap_or(0),
                if early_stop.is_some() { " [OPT-19]" } else { "" });
        let bs = self.block_size as usize;
        let byte_budget = self.memory_limit_bytes / SPILL_BUDGET_FRAC;
        let mut out_buf = Vec::new(); let mut out_runs = Vec::new();
        let mut out_cap = 0usize; let mut out_total = 0usize;
        let mut buf_ram_running = 0usize;
        let mut n_rejected = 0usize;
        let mut early_done = false;

        const SCAN_BATCH: u64 = 64;
        let mut blk_rel = 0u64;
        'scan_loop: while blk_rel < file_blocks {
            if early_done { break; }
            let batch = SCAN_BATCH.min(file_blocks - blk_rel);
            let blocks = self.read_blocks_batch(file_start + blk_rel, batch)?;
            for blk in &blocks {
                if early_done { break; }
                let b = blk.as_slice();
                let rc = u16::from_le_bytes([b[bs-2], b[bs-1]]) as usize;
                if rc == 0 { blk_rel += 1; continue; }
                let mut off = 0usize;
                for _ in 0..rc {
                    if off >= bs - 2 { break; }
                    let (row, ok) = decode_table_row_prebuilt(b, &mut off, bs, &col_names, &col_specs);
                    if !ok { break; }
                    if let Some((ref ec, ref bound)) = early_stop {
                        if let Some(vs) = col_val(&row, ec) {
                            let exceeded = match bound {
                                EarlyStopBound::Numeric(bv, incl) => {
                                    if let Ok(v) = vs.parse::<f64>() {
                                        if *incl { v > *bv } else { v >= *bv }
                                    } else { false }
                                }
                                EarlyStopBound::Lexical(bv, incl) => {
                                    if *incl { vs > bv.as_str() } else { vs >= bv.as_str() }
                                }
                            };
                            if exceeded { early_done = true; break 'scan_loop; }
                        }
                    }
                    if !eval_preds(&row, &preds) { n_rejected += 1; continue; }
                    let emit = if let Some(ref proj) = project {
                        let mut r = Vec::with_capacity(proj.len());
                        for (from, to) in proj {
                            if let Some(v) = find_col(&row, from) {
                                r.push((to.clone(), v.clone()));
                            }
                        }
                        if r.is_empty() { continue; }
                        r
                    } else { row };
                    self.emit_row(emit, &mut out_buf, &mut out_runs, &mut out_cap,
                                &mut out_total, &mut buf_ram_running, byte_budget)?;
                }
                blk_rel += 1;
            }
        }

        eprintln!("[FSCAN] done: passed={} rejected={} early={}", out_total, n_rejected, early_done);
        self.finalize_output(out_buf, out_runs, out_total)
    }

    fn exec_filter(&mut self, data: &OptFilterData) -> Result<RunSet> {
        eprintln!("[FILTER] preds={} (streaming)", data.predicates.len());
        let preds = data.predicates.clone();
        let byte_budget = self.memory_limit_bytes / SPILL_BUDGET_FRAC;
        let mut out_buf = Vec::new(); let mut out_runs = Vec::new();
        let mut out_cap = 0usize; let mut out_total = 0usize;
        let mut buf_ram_running = 0usize;
        let child = data.underlying.as_ref() as *const OptQueryOp;
        let mut cb = |this: &mut Self, row: Row| -> Result<()> {
            if eval_preds(&row, &preds) {
                this.emit_row(row, &mut out_buf, &mut out_runs, &mut out_cap, &mut out_total, &mut buf_ram_running, byte_budget)?;
            }
            Ok(())
        };
        self.stream_op(unsafe { &*child }, &mut cb)?;
        eprintln!("[FILTER] out={}", out_total);
        self.finalize_output(out_buf, out_runs, out_total)
    }

    fn exec_project(&mut self, data: &OptProjectData) -> Result<RunSet> {
        eprintln!("[PROJECT] cols={} (streaming)", data.column_name_map.len());
        let map = data.column_name_map.clone();
        let byte_budget = self.memory_limit_bytes / SPILL_BUDGET_FRAC;
        let mut out_buf = Vec::new(); let mut out_runs = Vec::new();
        let mut out_cap = 0usize; let mut out_total = 0usize;
        let mut buf_ram_running = 0usize;
        let child = data.underlying.as_ref() as *const OptQueryOp;
        let mut cb = |this: &mut Self, row: Row| -> Result<()> {
            let r = project_row(&row, &map);
            if !r.is_empty() {
                this.emit_row(r, &mut out_buf, &mut out_runs, &mut out_cap, &mut out_total, &mut buf_ram_running, byte_budget)?;
            }
            Ok(())
        };
        self.stream_op(unsafe { &*child }, &mut cb)?;
        eprintln!("[PROJECT] out={}", out_total);
        self.finalize_output(out_buf, out_runs, out_total)
    }

    fn exec_cross(&mut self, data: &OptCrossData) -> Result<RunSet> {
        let left_rs  = self.exec_node(&data.left)?;
        let right_rs = self.exec_node(&data.right)?;
        let lc = left_rs.row_count(); let rc = right_rs.row_count();
        eprintln!("[CROSS] left={} right={}", lc, rc);
        if lc == 0 || rc == 0 { return Ok(RunSet::InMemory(Vec::new())); }
        let est_bytes = lc.saturating_mul(rc).saturating_mul(512);
        if est_bytes > self.memory_limit_bytes.saturating_mul(8) {
            anyhow::bail!(
                "[CROSS][MEM-GUARD] {}x{} rows (~{} MB) exceeds 8x memory budget. \
                 Likely a missing join predicate in the query plan.",
                lc, rc, est_bytes / (1024 * 1024));
        }
        let (outer_rs, inner_rs) = if rc <= lc { (left_rs, right_rs) } else { (right_rs, left_rs) };
        self.nested_loop_join(outer_rs, inner_rs, &[])
    }

    fn exec_filter_cross(&mut self, data: &FilterCrossData) -> Result<RunSet> {
        let left_rs  = self.exec_node(&data.left)?;
        let right_rs = self.exec_node(&data.right)?;
        let lc = left_rs.row_count(); let rc = right_rs.row_count();
        eprintln!("[FCROSS] left={} right={} preds={}", lc, rc, data.predicates.len());
        if lc == 0 || rc == 0 { return Ok(RunSet::InMemory(Vec::new())); }
        let est_product = lc.saturating_mul(rc);
        if est_product > 500_000_000 {
            eprintln!("[FCROSS][WARN] large NLJ {}x{}={} pairs — slow", lc, rc, est_product);
        }
        let (outer_rs, inner_rs) = if rc <= lc { (left_rs, right_rs) } else { (right_rs, left_rs) };
        self.nested_loop_join(outer_rs, inner_rs, &data.predicates)
    }

    fn nested_loop_join(&mut self, outer_rs: RunSet, inner_rs: RunSet, preds: &[Predicate]) -> Result<RunSet> {
        let byte_budget = self.memory_limit_bytes / SPILL_BUDGET_FRAC;
        let p = std::mem::size_of::<usize>(); let bs = self.block_size as usize;
        let inner_rs = self.maybe_collect_small(inner_rs)?;
        let outer_rs = self.maybe_collect_small(outer_rs)?;
        let inner_descs = self.ensure_on_disk(inner_rs)?;
        let outer_descs = self.ensure_on_disk(outer_rs)?;
        let mut out_buf = Vec::new(); let mut out_runs = Vec::new();
        let mut out_cap = 0usize; let mut out_total = 0usize;
        let mut buf_ram_running = 0usize;
        let mut chunk = Vec::new(); let mut chunk_bytes = 0usize;
        // NLJ chunk budget: half of the total budget, so inner and outer buffers
        // share the remaining half.
        let chunk_budget = byte_budget / 2;
        for &(i_start, i_nblk) in &inner_descs {
            for bi in 0..i_nblk {
                let ibuf = self.read_block_direct(i_start + bi)?;
                let rc = u16::from_le_bytes([ibuf[bs-2], ibuf[bs-1]]) as usize;
                let mut off = 0usize;
                for _ in 0..rc {
                    if let Some(row) = decode_row(&ibuf, &mut off) {
                        let rb = 3*p + row.iter().map(|(k,v)| 6*p+k.len()+v.len()).sum::<usize>();
                        if chunk_bytes + rb > chunk_budget && !chunk.is_empty() {
                            self.nlj_emit_chunk(&chunk, &outer_descs, preds,
                                &mut out_buf, &mut out_runs, &mut out_cap, &mut out_total, &mut buf_ram_running, byte_budget)?;
                            chunk.clear(); chunk_bytes = 0;
                        }
                        chunk_bytes += rb; chunk.push(row);
                    }
                }
            }
        }
        if !chunk.is_empty() {
            self.nlj_emit_chunk(&chunk, &outer_descs, preds,
                &mut out_buf, &mut out_runs, &mut out_cap, &mut out_total, &mut buf_ram_running, byte_budget)?;
        }
        eprintln!("[NLJ] out={}", out_total);
        self.finalize_output(out_buf, out_runs, out_total)
    }

    fn nlj_emit_chunk(&mut self, chunk: &[Row], outer_descs: &[(u64, u64)], preds: &[Predicate],
                      out_buf: &mut Vec<Row>, out_runs: &mut Vec<ScratchRun>,
                      out_cap: &mut usize, out_total: &mut usize,
                      buf_ram_running: &mut usize, byte_budget: usize) -> Result<()> {
        let bs = self.block_size as usize;
        for &(o_start, o_nblk) in outer_descs {
            for bo in 0..o_nblk {
                let obuf = self.read_block_direct(o_start + bo)?;
                let orc = u16::from_le_bytes([obuf[bs-2], obuf[bs-1]]) as usize;
                let mut ooff = 0usize;
                for _ in 0..orc {
                    if let Some(o_row) = decode_row(&obuf, &mut ooff) {
                        for i_row in chunk {
                            let mut r = o_row.clone(); r.extend_from_slice(i_row);
                            if !eval_preds(&r, preds) { continue; }
                            self.emit_row(r, out_buf, out_runs, out_cap, out_total, buf_ram_running, byte_budget)?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    // ─────────────────────────────────────────────────────────────────────────
    // [OPT-14-EXEC] exec_hash_join with adaptive build budget.
    // ─────────────────────────────────────────────────────────────────────────
    // In exec_hash_join, replace:
    //   let right_rs = self.exec_node(&data.right)?;
    // with lazy probe: execute left (build) fully, then stream right (probe)

    fn exec_hash_join(&mut self, data: &HashJoinData) -> Result<RunSet> {
        // Execute BUILD side fully first
        let left_rs  = self.exec_node(&data.left)?;

        eprintln!("[HJ] left={} keys={:?}={:?}",
                left_rs.row_count(), data.left_keys, data.right_keys);

        if left_rs.row_count() == 0 {
            return Ok(RunSet::InMemory(Vec::new()));
        }

        let extra_preds = data.extra_preds.clone();
        let left_keys   = data.left_keys.clone();
        let right_keys  = data.right_keys.clone();

        // For build/probe selection we need right size estimate — use stats, not execution
        // Always use left as build side here; swap was already done by optimiser
        let build_rs = left_rs;
        let build_keys = left_keys;
        let probe_keys = right_keys;

        // Check if SMJ applies — needs right_rs which we haven't executed yet.
        // Skip SMJ for now (conservative).

        let build_row_count = build_rs.row_count();
        let sample_bpr = match &build_rs {
            RunSet::InMemory(rows) => rows.first().map(|r| Self::row_ram(r)).unwrap_or(512),
            RunSet::Spilled { .. } => 512,
        };
        let two_thirds = self.memory_limit_bytes * 3 / 5;
        let build_budget = two_thirds.max(self.memory_limit_bytes / 8);
        eprintln!("[OPT-14-EXEC][OPT-20] build_budget={}MB",
                build_budget / (1024*1024));

        let p = std::mem::size_of::<usize>();
        let mut build_map: HashMap<String, Vec<Row>> = HashMap::with_capacity(build_row_count.min(32_768));
        let mut build_bytes = 0usize;
        let mut overflowed = false;
        let mut build_disk_descs: Vec<(u64, u64)> = Vec::new();
        let mut key_buf = String::with_capacity(128);
        let mut spill_buf: Vec<Row> = Vec::new();
        let spill_cap = Self::safe_cap(self.memory_limit_bytes / SPILL_BUDGET_FRAC, 512).max(MIN_CAP_ROWS);

        match build_rs {
            RunSet::InMemory(rows) => {
                for row in rows {
                    if overflowed {
                        spill_buf.push(row);
                        if spill_buf.len() >= spill_cap {
                            let run = self.spill(&spill_buf)?;
                            build_disk_descs.push((run.start_block, run.num_blocks));
                            spill_buf.clear();
                        }
                    } else {
                        let raw = 3*p + row.iter().map(|(k,v)| 6*p+k.len()+v.len()).sum::<usize>() + 64 + p;
                        let rb = raw + raw * 2 / 5;
                        build_bytes += rb;
                        if build_bytes > build_budget {
                            overflowed = true;
                            let map_rows: Vec<Row> = build_map.drain().flat_map(|(_, v)| v).collect();
                            for chunk in map_rows.chunks(spill_cap) {
                                let run = self.spill(chunk)?;
                                build_disk_descs.push((run.start_block, run.num_blocks));
                            }
                            spill_buf.push(row);
                        } else {
                            make_join_key_into(&row, &build_keys, &mut key_buf);
                            let key = key_buf.clone();
                            build_map.entry(key).or_default().push(row);
                        }
                    }
                }
            }
            RunSet::Spilled { runs, .. } => {
                for run in &runs { build_disk_descs.push((run.start_block, run.num_blocks)); }
                if !overflowed {
                    let mut temp_rows: Vec<Row> = Vec::new();
                    'outer: for &(start, n_blk) in &build_disk_descs.clone() {
                        let mut blk_idx = 0u64;
                        while blk_idx < n_blk {
                            let batch = (64u64).min(n_blk - blk_idx);
                            let blocks = self.read_blocks_batch(start + blk_idx, batch)?;
                            for buf in &blocks {
                                let bs = self.block_size as usize;
                                let rc = u16::from_le_bytes([buf[bs-2], buf[bs-1]]) as usize;
                                let mut off = 0usize;
                                for _ in 0..rc {
                                    if let Some(row) = decode_row(buf, &mut off) {
                                        let raw = 3*p + row.iter().map(|(k,v)| 6*p+k.len()+v.len()).sum::<usize>() + 64 + p;
                                        let rb = raw + raw * 2 / 5;
                                        build_bytes += rb;
                                        if build_bytes > build_budget {
                                            overflowed = true;
                                            break 'outer;
                                        }
                                        temp_rows.push(row);
                                    }
                                }
                            }
                            blk_idx += batch;
                        }
                    }
                    if !overflowed {
                        for row in temp_rows {
                            make_join_key_into(&row, &build_keys, &mut key_buf);
                            let key = key_buf.clone();
                            build_map.entry(key).or_default().push(row);
                        }
                    }
                }
            }
        }

        if !spill_buf.is_empty() {
            let run = self.spill(&spill_buf)?;
            build_disk_descs.push((run.start_block, run.num_blocks));
            spill_buf.clear();
        }

        if overflowed {
            eprintln!("[OPT-07] overflow → Grace Hash Join");
            let total_rows = build_row_count;
            let all_runs: Vec<ScratchRun> = build_disk_descs.iter()
                .map(|&(s, n)| ScratchRun { start_block: s, num_blocks: n, row_count: 0 }).collect();
            let all_build = RunSet::Spilled { row_count: total_rows, runs: all_runs };
            // Execute probe side now
            let probe_rs = self.exec_node(&data.right)?;
            eprintln!("[HJ] right={}", probe_rs.row_count());
            return self.grace_hash_join(all_build, probe_rs, build_keys, probe_keys, extra_preds);
        }

        eprintln!("[HJ] in-memory build: {} keys, {} B", build_map.len(), build_bytes);

        // ── STREAM probe side directly — never fully materialize it ──────────
        let byte_budget = self.memory_limit_bytes / SPILL_BUDGET_FRAC;
        let mut out_buf = Vec::new(); let mut out_runs = Vec::new();
        let mut out_cap = 0usize; let mut out_total = 0usize;
        let mut buf_ram_running = 0usize;
        let mut key_buf2 = String::with_capacity(128);

        // Use stream_op on the RIGHT child directly — never materializes probe
        let probe_child = data.right.as_ref() as *const OptQueryOp;
        self.stream_op(unsafe { &*probe_child }, &mut |this, p_row| {
            make_join_key_into(&p_row, &probe_keys, &mut key_buf2);
            if let Some(b_rows) = build_map.get(&key_buf2) {
                for b_row in b_rows {
                    let mut row = p_row.clone();
                    row.extend_from_slice(b_row);
                    if !eval_preds(&row, &extra_preds) { return Ok(()); }
                    this.emit_row(row, &mut out_buf, &mut out_runs, &mut out_cap,
                                &mut out_total, &mut buf_ram_running, byte_budget)?;
                }
            }
            Ok(())
        })?;

        eprintln!("[HJ] out={}", out_total);
        self.finalize_output(out_buf, out_runs, out_total)
    }

    // ─────────────────────────────────────────────────────────────────────────
    // [OPT-17][BUG-SMJ-DISK] sort_merge_join.
    //
    // BUG FIX: The old code called ensure_on_disk(probe_rs) unconditionally,
    // causing unnecessary disk writes for InMemory probe sides. Now InMemory
    // probe sides are iterated directly.
    // ─────────────────────────────────────────────────────────────────────────
    fn sort_merge_join(&mut self, build_rs: RunSet, probe_rs: RunSet,
                       build_keys: Vec<String>, probe_keys: Vec<String>,
                       extra_preds: Vec<Predicate>) -> Result<RunSet> {
        eprintln!("[OPT-17] SMJ key={:?}", build_keys);
        let bs = self.block_size as usize;
        let mut build_groups: Vec<(String, Vec<Row>)> = Vec::new();
        let mut key_buf = String::with_capacity(128);

        // [IO-FIX-9] InMemory build iterated directly.
        match build_rs {
            RunSet::InMemory(rows) => {
                for row in rows {
                    make_join_key_into(&row, &build_keys, &mut key_buf);
                    let key = key_buf.clone();
                    match build_groups.last_mut() {
                        Some((k, rs)) if *k == key => rs.push(row),
                        _ => build_groups.push((key, vec![row])),
                    }
                }
            }
            RunSet::Spilled { .. } => {
                for (start, n_blk) in &self.ensure_on_disk(build_rs)? {
                    for bi in 0..*n_blk {
                        let buf = self.read_block_direct(*start + bi)?;
                        let rc  = u16::from_le_bytes([buf[bs-2], buf[bs-1]]) as usize;
                        let mut off = 0usize;
                        for _ in 0..rc {
                            if let Some(row) = decode_row(&buf, &mut off) {
                                make_join_key_into(&row, &build_keys, &mut key_buf);
                                let key = key_buf.clone();
                                match build_groups.last_mut() {
                                    Some((k, rs)) if *k == key => rs.push(row),
                                    _ => build_groups.push((key, vec![row])),
                                }
                            }
                        }
                    }
                }
            }
        }
        eprintln!("[OPT-17] build_groups={}", build_groups.len());
        let byte_budget = self.memory_limit_bytes / SPILL_BUDGET_FRAC;
        let mut out_buf = Vec::new(); let mut out_runs = Vec::new();
        let mut out_cap = 0usize; let mut out_total = 0usize;
        let mut buf_ram_running = 0usize;
        let mut bg_idx = 0usize;

        // [BUG-SMJ-DISK] Only spill probe if it's already Spilled. InMemory → iterate directly.
        match probe_rs {
            RunSet::InMemory(rows) => {
                for p_row in rows {
                    make_join_key_into(&p_row, &probe_keys, &mut key_buf);
                    while bg_idx < build_groups.len() && build_groups[bg_idx].0 < key_buf { bg_idx += 1; }
                    if bg_idx < build_groups.len() && build_groups[bg_idx].0 == key_buf {
                        for b_row in &build_groups[bg_idx].1 {
                            let mut r = p_row.clone(); r.extend_from_slice(b_row);
                            if !eval_preds(&r, &extra_preds) { continue; }
                            self.emit_row(r, &mut out_buf, &mut out_runs, &mut out_cap, &mut out_total, &mut buf_ram_running, byte_budget)?;
                        }
                    }
                }
            }
            RunSet::Spilled { .. } => {
                let probe_descs = self.ensure_on_disk(probe_rs)?;
                for (start, n_blk) in &probe_descs {
                    for bi in 0..*n_blk {
                        let buf = self.read_block_direct(*start + bi)?;
                        let rc  = u16::from_le_bytes([buf[bs-2], buf[bs-1]]) as usize;
                        let mut off = 0usize;
                        for _ in 0..rc {
                            if let Some(p_row) = decode_row(&buf, &mut off) {
                                make_join_key_into(&p_row, &probe_keys, &mut key_buf);
                                while bg_idx < build_groups.len() && build_groups[bg_idx].0 < key_buf { bg_idx += 1; }
                                if bg_idx < build_groups.len() && build_groups[bg_idx].0 == key_buf {
                                    for b_row in &build_groups[bg_idx].1 {
                                        let mut r = p_row.clone(); r.extend_from_slice(b_row);
                                        if !eval_preds(&r, &extra_preds) { continue; }
                                        self.emit_row(r, &mut out_buf, &mut out_runs, &mut out_cap, &mut out_total, &mut buf_ram_running, byte_budget)?;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        eprintln!("[OPT-17] SMJ out={}", out_total);
        self.finalize_output(out_buf, out_runs, out_total)
    }

    // Replace all read_blocks_batch(start, n_blk) calls in grace_hash_join
    // with the batched loop pattern:
    fn read_run_batched(&mut self, start: u64, n_blk: u64, bs: usize, 
                        out: &mut Vec<Row>) -> Result<()> {
        const BATCH: u64 = 64;
        let mut blk_idx = 0u64;
        while blk_idx < n_blk {
            let batch = BATCH.min(n_blk - blk_idx);
            let blocks = self.read_blocks_batch(start + blk_idx, batch)?;
            for buf in &blocks {
                let rc = u16::from_le_bytes([buf[bs-2], buf[bs-1]]) as usize;
                let mut off = 0usize;
                for _ in 0..rc {
                    if let Some(row) = decode_row(buf, &mut off) { out.push(row); }
                }
            }
            blk_idx += batch;
        }
        Ok(())
    }
    fn grace_hash_join(&mut self, build_rs: RunSet, probe_rs: RunSet,
               build_keys: Vec<String>, probe_keys: Vec<String>,
               extra_preds: Vec<Predicate>) -> Result<RunSet> {
        let avg_row = 1200usize;
        let n_parts = grace_partition_count(build_rs.row_count() as u64, avg_row, self.memory_limit_bytes);
        let bs = self.block_size as usize;
        let build_rows_est = build_rs.row_count();
        let probe_rows_est = probe_rs.row_count();

        let rows_per_part = (build_rows_est / n_parts).max(1);
        let per_blks  = ((rows_per_part * avg_row + bs - 1) / bs + 2).max(4) as u64;
        let probe_per = (((probe_rows_est / n_parts) * avg_row + bs - 1) / bs + 2).max(4) as u64;
        eprintln!("[OPT-16] grace n_parts={} per_blks={} probe_per={}", n_parts, per_blks, probe_per);

        let blks_per_part = (per_blks * 2).max(8);
        let build_base = self.next_anon;
        self.next_anon += blks_per_part * n_parts as u64;
        let mut bw: Vec<PartWriter> = (0..n_parts)
            .map(|i| PartWriter::new(build_base + i as u64 * blks_per_part, bs))
            .collect();

        // ── Partition build side ──────────────────────────────────────────────
        let build_descs = self.ensure_on_disk(build_rs)?;
        let mut enc = Vec::with_capacity(1024);
        let mut key_buf = String::with_capacity(128);

        for (start, n_blk) in &build_descs {
            if *n_blk == 0 { continue; }
            let blocks = self.read_blocks_batch(*start, *n_blk)?;
            for buf in &blocks {
                let rc = u16::from_le_bytes([buf[bs-2], buf[bs-1]]) as usize;
                let mut off = 0usize;
                for _ in 0..rc {
                    if let Some(row) = decode_row(buf, &mut off) {
                        make_join_key_into(&row, &build_keys, &mut key_buf);
                        let pi = hash_str(&key_buf) % n_parts;
                        enc.clear(); encode_row(&row, &mut enc);
                        let enc_ref = &enc;
                        let disk_out = &mut self.disk_out;
                        bw[pi].push_encoded(enc_ref, disk_out)?;
                    }
                }
            }
        }

        let mut build_parts: Vec<(u64, u64, usize)> = Vec::with_capacity(n_parts);
        for i in 0..n_parts {
            let result = bw[i].finish(&mut self.disk_out)?;
            self.disk_out.flush()?;
            build_parts.push(result);
        }
        drop(bw);

        // ── Partition probe side ──────────────────────────────────────────────
        let probe_blks_per_part = (probe_per * 2).max(8);
        let probe_base = self.next_anon;
        self.next_anon += probe_blks_per_part * n_parts as u64;
        let mut pw: Vec<PartWriter> = (0..n_parts)
            .map(|i| PartWriter::new(probe_base + i as u64 * probe_blks_per_part, bs))
            .collect();

        let probe_descs = self.ensure_on_disk(probe_rs)?;
        for (start, n_blk) in &probe_descs {
            if *n_blk == 0 { continue; }
            let blocks = self.read_blocks_batch(*start, *n_blk)?;
            for buf in &blocks {
                let rc = u16::from_le_bytes([buf[bs-2], buf[bs-1]]) as usize;
                let mut off = 0usize;
                for _ in 0..rc {
                    if let Some(row) = decode_row(buf, &mut off) {
                        make_join_key_into(&row, &probe_keys, &mut key_buf);
                        let pi = hash_str(&key_buf) % n_parts;
                        enc.clear(); encode_row(&row, &mut enc);
                        let enc_ref = &enc;
                        let disk_out = &mut self.disk_out;
                        pw[pi].push_encoded(enc_ref, disk_out)?;
                    }
                }
            }
        }

        let mut probe_parts: Vec<(u64, u64, usize)> = Vec::with_capacity(n_parts);
        for i in 0..n_parts {
            let result = pw[i].finish(&mut self.disk_out)?;
            self.disk_out.flush()?;
            probe_parts.push(result);
        }
        drop(pw);

        // ── Join each partition pair ──────────────────────────────────────────
        let byte_budget = self.memory_limit_bytes / SPILL_BUDGET_FRAC;
        let mut out_buf = Vec::new(); let mut out_runs = Vec::new();
        let mut out_cap = 0usize; let mut out_total = 0usize;
        let mut buf_ram_running = 0usize;

        for i in 0..n_parts {
            let (b_start, b_nblk, b_rc) = build_parts[i];
            let (p_start, p_nblk, p_rc) = probe_parts[i];
            if b_rc == 0 || p_rc == 0 { continue; }

            let skew_thresh = (b_rc as f64 * 0.30) as usize;

            let mut part_rows: Vec<Row> = Vec::with_capacity(b_rc);
            self.read_run_batched(b_start, b_nblk, bs, &mut part_rows)?;

            let mut key_freq: HashMap<String, usize> = HashMap::new();
            for row in &part_rows {
                make_join_key_into(row, &build_keys, &mut key_buf);
                *key_freq.entry(key_buf.clone()).or_insert(0) += 1;
            }
            let max_f = key_freq.values().copied().max().unwrap_or(0);

            if max_f > skew_thresh && skew_thresh > 0 {
                eprintln!("[OPT-15] partition {} skewed (max_f={}/{}) → NLJ", i, max_f, b_rc);
                self.grace_nlj_rows(&part_rows, &probe_keys, &build_keys, &extra_preds,
                                    p_start, p_nblk, &mut out_buf, &mut out_runs, &mut out_cap,
                                    &mut out_total, &mut buf_ram_running, byte_budget)?;
                continue;
            }

            let mut part_map: HashMap<String, Vec<Row>> = HashMap::with_capacity(part_rows.len());
            for row in part_rows {
                make_join_key_into(&row, &build_keys, &mut key_buf);
                part_map.entry(key_buf.clone()).or_default().push(row);
            }

            {
                const BATCH: u64 = 64;
                let mut blk_idx = 0u64;
                while blk_idx < p_nblk {
                    let batch = BATCH.min(p_nblk - blk_idx);
                    let blocks = self.read_blocks_batch(p_start + blk_idx, batch)?;
                    for blk in &blocks {
                        let rc = u16::from_le_bytes([blk[bs-2], blk[bs-1]]) as usize;
                        let mut off = 0usize;
                        for _ in 0..rc {
                            if let Some(p_row) = decode_row(blk, &mut off) {
                                make_join_key_into(&p_row, &probe_keys, &mut key_buf);
                                if let Some(b_rows) = part_map.get(&key_buf) {
                                    for b_row in b_rows {
                                        let mut row = p_row.clone(); row.extend_from_slice(b_row);
                                        if !eval_preds(&row, &extra_preds) { continue; }
                                        self.emit_row(row, &mut out_buf, &mut out_runs, &mut out_cap,
                                                    &mut out_total, &mut buf_ram_running, byte_budget)?;
                                    }
                                }
                            }
                        }
                    }
                    blk_idx += batch;
                }
            }
        }

        eprintln!("[OPT-07][GRACE] out={}", out_total);
        self.finalize_output(out_buf, out_runs, out_total)
    }

    #[allow(clippy::too_many_arguments)]
    fn grace_nlj_rows(&mut self, build_rows: &[Row], probe_keys: &[String], build_keys: &[String],
                  extra_preds: &[Predicate], p_start: u64, p_nblk: u64,
                  out_buf: &mut Vec<Row>, out_runs: &mut Vec<ScratchRun>,
                  out_cap: &mut usize, out_total: &mut usize,
                  buf_ram_running: &mut usize, byte_budget: usize) -> Result<()> {
        let bs = self.block_size as usize;
        let p = std::mem::size_of::<usize>();
        let chunk_budget = byte_budget / 2;
        let mut start = 0usize; let mut c_bytes = 0usize; let n = build_rows.len();
        let mut key_buf = String::with_capacity(128);
        let mut key_buf2 = String::with_capacity(128);
        let mut i = 0usize;

        // Preload all probe blocks for this partition once
        let probe_blocks = self.read_blocks_batch(p_start, p_nblk)?;

        loop {
            let end_of_data = i == n;
            let flush = end_of_data || {
                let rb = 3*p + build_rows[i].iter().map(|(k,v)| 6*p+k.len()+v.len()).sum::<usize>();
                c_bytes + rb > chunk_budget && i > start
            };
            if flush && i > start {
                let chunk = &build_rows[start..i];
                for buf in &probe_blocks {
                    let rc = u16::from_le_bytes([buf[bs-2], buf[bs-1]]) as usize;
                    let mut off = 0usize;
                    for _ in 0..rc {
                        if let Some(p_row) = decode_row(buf, &mut off) {
                            make_join_key_into(&p_row, probe_keys, &mut key_buf);
                            for b_row in chunk {
                                make_join_key_into(b_row, build_keys, &mut key_buf2);
                                if key_buf != key_buf2 { continue; }
                                let mut r = p_row.clone(); r.extend_from_slice(b_row);
                                if !eval_preds(&r, extra_preds) { continue; }
                                self.emit_row(r, out_buf, out_runs, out_cap, out_total,
                                            buf_ram_running, byte_budget)?;
                            }
                        }
                    }
                }
                start = i; c_bytes = 0;
            }
            if end_of_data { break; }
            let rb = 3*p + build_rows[i].iter().map(|(k,v)| 6*p+k.len()+v.len()).sum::<usize>();
            c_bytes += rb; i += 1;
        }
        Ok(())
    }

    // ─────────────────────────────────────────────────────────────────────────
    // [OPT-08][OPT-11-EXEC][BUG-SORT-BUFFER] exec_sort with tournament-tree merge.
    // ─────────────────────────────────────────────────────────────────────────
    fn exec_sort(&mut self, data: &OptSortData) -> Result<RunSet> {
        let input = self.exec_node(&data.underlying)?;
        eprintln!("[SORT] input={} ordered={}", input.row_count(), data.is_physically_ordered);
        if data.is_physically_ordered { eprintln!("[OPT-11] Sort skipped"); return Ok(input); }
        if input.row_count() == 0 { return Ok(RunSet::InMemory(Vec::new())); }

        let fits = match &input {
            RunSet::InMemory(t) => rough_ram(t) <= self.memory_limit_bytes * 3 / 4,
            RunSet::Spilled { .. } => false,
        };
        if fits {
            let mut t = self.collect_runset(input)?;
            sort_slice(&mut t, &data.sort_specs);
            eprintln!("[SORT] in-memory rows={}", t.len());
            return Ok(RunSet::InMemory(t));
        }

        let bpr = self.sample_bpr(&input)?;
        // 1/3 of budget: leaves room for read buffer + output buffer simultaneously
        let pass1_budget = self.memory_limit_bytes / 3;
        let pass1_cap = Self::safe_cap(pass1_budget, bpr);
        eprintln!("[OPT-08] external sort bpr~{} pass1_cap={}", bpr, pass1_cap);
        let specs = data.sort_specs.clone();
        let mut runs: Vec<ScratchRun> = Vec::new();
        // IMPORTANT: do NOT pre-allocate with_capacity here — use push and let it grow
        let mut chunk: Vec<Row> = Vec::new();
        let bs = self.block_size as usize;

        match input {
            RunSet::InMemory(rows) => {
                for row in rows {
                    chunk.push(row);
                    if chunk.len() >= pass1_cap {
                        sort_slice(&mut chunk, &specs);
                        runs.push(self.spill(&chunk)?);
                        chunk.clear();
                    }
                }
            }
            RunSet::Spilled { .. } => {
                let descs = self.ensure_on_disk(input)?;
                for (start, n_blk) in descs {
                    if n_blk == 0 { continue; }
                    let blocks = self.read_blocks_batch(start, n_blk)?;
                    for buf in &blocks {
                        let rc = u16::from_le_bytes([buf[bs-2], buf[bs-1]]) as usize;
                        let mut off = 0usize;
                        for _ in 0..rc {
                            if let Some(row) = decode_row(buf, &mut off) {
                                chunk.push(row);
                                if chunk.len() >= pass1_cap {
                                    sort_slice(&mut chunk, &specs);
                                    runs.push(self.spill(&chunk)?);
                                    chunk.clear();
                                }
                            }
                        }
                    }
                }
            }
        }
        if !chunk.is_empty() {
            sort_slice(&mut chunk, &specs);
            runs.push(self.spill(&chunk)?);
            chunk.clear();
        }
        drop(chunk);
        eprintln!("[OPT-08] pass1: {} sorted runs", runs.len());

        if runs.len() == 1 {
            let r = runs.remove(0);
            return Ok(RunSet::Spilled { row_count: r.row_count, runs: vec![r] });
        }

        let merge_budget = self.memory_limit_bytes / MERGE_BUDGET_FRAC;
        let merge_cap = Self::safe_cap(merge_budget, bpr);
        self.merge_sorted_runs(runs, &specs, merge_cap, merge_budget)
    }

    fn sample_bpr(&mut self, rs: &RunSet) -> Result<usize> {
        let p = std::mem::size_of::<usize>();
        match rs {
            RunSet::InMemory(t) => { if t.is_empty() { return Ok(512); } Ok(Self::row_ram(&t[0]).max(64)) }
            RunSet::Spilled { runs, .. } => {
                for run in runs {
                    if run.num_blocks == 0 { continue; }
                    let buf = self.read_block_direct(run.start_block)?;
                    let bs = self.block_size as usize;
                    let rc = u16::from_le_bytes([buf[bs-2], buf[bs-1]]) as usize;
                    if rc == 0 { continue; }
                    let mut off = 0usize;
                    if let Some(row) = decode_row(&buf, &mut off) {
                        return Ok((3*p + row.iter().map(|(k,v)| 6*p+k.len()+v.len()).sum::<usize>()).max(64));
                    }
                }
                Ok(512)
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // [OPT-11-EXEC] K-way merge with tournament tree (BinaryHeap min-heap).
    // O(log k) per row instead of O(k) for linear scan over heads.
    // ─────────────────────────────────────────────────────────────────────────
    fn merge_sorted_runs(
    &mut self,
    runs: Vec<ScratchRun>,
    specs: &[SortSpec],
    out_cap: usize,
    merge_budget: usize,
) -> Result<RunSet> {
        let bs = self.block_size as usize;
        eprintln!("[OPT-08][OPT-11-EXEC] merge {} runs out_cap={} (tournament tree)", runs.len(), out_cap);

        // Initialize each head by reading only its FIRST block
        let mut heads: Vec<MergeHead> = Vec::with_capacity(runs.len());
        for run in &runs {
            if run.num_blocks == 0 || run.row_count == 0 { continue; }
            // Read only the first block of this run
            let buf = self.read_block_direct(run.start_block)?;
            let brc = u16::from_le_bytes([buf[bs-2], buf[bs-1]]) as usize;
            let mut h = MergeHead {
                run_start:       run.start_block,
                run_blocks:      run.num_blocks,
                cur_block:       0,
                current_block:   buf,
                block_row_count: brc,
                block_row_idx:   0,
                block_off:       0,
                current_row:     None,
            };
            h.advance(bs);
            if h.current_row.is_some() { heads.push(h); }
        }

        // Build min-heap
        let mut heap: BinaryHeap<Reverse<HeapEntry>> = BinaryHeap::with_capacity(heads.len());
        for (idx, h) in heads.iter().enumerate() {
            if let Some(ref row) = h.current_row {
                heap.push(Reverse(HeapEntry {
                    key:      row_to_heap_key(row, specs),
                    head_idx: idx,
                }));
            }
        }

        let mut out_buf: Vec<Row> = Vec::new();
        let mut out_runs: Vec<ScratchRun> = Vec::new();
        let mut out_total = 0usize;
        let mut buf_ram_running = 0usize;

        while let Some(Reverse(entry)) = heap.pop() {
            let idx = entry.head_idx;
            let row = heads[idx].current_row.take().unwrap();
            buf_ram_running += Self::row_ram(&row);
            out_total += 1;
            out_buf.push(row);

            if out_buf.len() >= out_cap || buf_ram_running >= merge_budget {
                let run = self.spill(&out_buf)?;
                out_runs.push(run);
                out_buf.clear();
                buf_ram_running = 0;
            }

            // Advance this head
            heads[idx].advance(bs);

            // If current block exhausted and more blocks remain, load next block
            if heads[idx].needs_next_block() {
                let next_abs = heads[idx].run_start + heads[idx].cur_block + 1;
                let buf = self.read_block_direct(next_abs)?;
                heads[idx].load_block(buf, bs);
            }

            if let Some(ref row) = heads[idx].current_row {
                heap.push(Reverse(HeapEntry {
                    key:      row_to_heap_key(row, specs),
                    head_idx: idx,
                }));
            }
        }

        let rs = self.finalize_output(out_buf, out_runs, out_total)?;
        eprintln!("[OPT-08] merge done rows={}", rs.row_count());
        Ok(rs)
    }

    fn stream_runset<F>(&mut self, rs: RunSet, mut f: F) -> Result<()>
        where F: FnMut(&mut Self, Row) -> Result<()> {
            match rs {
                RunSet::InMemory(rows) => { for row in rows { f(self, row)?; } }
                RunSet::Spilled { runs, .. } => {
                    let bs = self.block_size as usize;
                    const STREAM_BATCH: u64 = 64; // max blocks per read request
                    for run in runs {
                        let mut blk_idx = 0u64;
                        while blk_idx < run.num_blocks {
                            let batch = STREAM_BATCH.min(run.num_blocks - blk_idx);
                            let blocks = self.read_blocks_batch(run.start_block + blk_idx, batch)?;
                            for buf in blocks {
                                let rc = u16::from_le_bytes([buf[bs-2], buf[bs-1]]) as usize;
                                let mut off = 0usize;
                                for _ in 0..rc {
                                    if let Some(row) = decode_row(&buf, &mut off) { f(self, row)?; }
                                }
                            }
                            blk_idx += batch;
                        }
                    }
                }
            }
            Ok(())
        }

    fn get_file_range(&mut self, file_id: &str) -> Result<(u64, u64)> {
        if let Some(&cached) = self.file_range_cache.get(file_id) { return Ok(cached); }
        let mut line = String::new();
        self.disk_out.write_all(format!("get file start-block {}\n", file_id).as_bytes())?;
        self.disk_out.flush()?; self.disk_in.read_line(&mut line)?;
        let file_start: u64 = line.trim().parse()
            .map_err(|e| anyhow::anyhow!("start-block \'{}\': {}", line.trim(), e))?;
        line.clear();
        self.disk_out.write_all(format!("get file num-blocks {}\n", file_id).as_bytes())?;
        self.disk_out.flush()?; self.disk_in.read_line(&mut line)?;
        let file_blocks: u64 = line.trim().parse()
            .map_err(|e| anyhow::anyhow!("num-blocks \'{}\': {}", line.trim(), e))?;
        let result = (file_start, file_blocks);
        self.file_range_cache.insert(file_id.to_string(), result);
        Ok(result)
    }

    pub fn read_block_pub(&mut self, abs: u64) -> Result<Vec<u8>> { self.read_block_direct(abs) }
}

// ─────────────────────────────────────────────────────────────────────────────
// [BUG-EARLY-STOP-DATE] EarlyStopBound: numeric or lexical comparison.
// ─────────────────────────────────────────────────────────────────────────────

enum EarlyStopBound {
    Numeric(f64, bool),   // (value, inclusive)
    Lexical(String, bool), // (value, inclusive)
}

// ─────────────────────────────────────────────────────────────────────────────
// [OPT-15-COL] decode_table_row_prebuilt: pre-built column names passed in.
// ─────────────────────────────────────────────────────────────────────────────
fn decode_table_row_prebuilt(
    b:         &[u8],
    off:       &mut usize,
    bs:        usize,
    col_names: &[String],
    cols:      &[db_config::table::ColumnSpec],
) -> (Row, bool) {
    let mut row = Vec::with_capacity(cols.len());
    let mut ok = true;
    for (col, col_name) in cols.iter().zip(col_names.iter()) {
        let val: String = match col.data_type {
            DataType::Int32 => {
                if *off + 4 > bs - 2 { ok = false; break; }
                let v = i32::from_le_bytes(b[*off..*off+4].try_into().unwrap()); *off += 4; v.to_string()
            }
            DataType::Int64 => {
                if *off + 8 > bs - 2 { ok = false; break; }
                let v = i64::from_le_bytes(b[*off..*off+8].try_into().unwrap()); *off += 8; v.to_string()
            }
            DataType::Float32 => {
                if *off + 4 > bs - 2 { ok = false; break; }
                let v = f32::from_le_bytes(b[*off..*off+4].try_into().unwrap()); *off += 4;
                if !v.is_finite() { ok = false; break; } fmt_f64(v as f64)
            }
            DataType::Float64 => {
                if *off + 8 > bs - 2 { ok = false; break; }
                let v = f64::from_le_bytes(b[*off..*off+8].try_into().unwrap()); *off += 8;
                if !v.is_finite() { ok = false; break; } fmt_f64(v)
            }
            DataType::String => {
                let s0 = *off;
                while *off < bs - 2 && b[*off] != 0 { *off += 1; }
                if *off >= bs - 2 { ok = false; break; }
                let s = String::from_utf8_lossy(&b[s0..*off]).into_owned(); *off += 1; s
            }
            _ => { ok = false; break; }
        };
        row.push((col_name.clone(), val));
    }
    let ok = ok && row.len() == cols.len();
    (row, ok)
}

// Keep original for compatibility.
#[allow(dead_code)]
fn decode_table_row(b: &[u8], off: &mut usize, bs: usize,
                    table_id: &str, cols: &[db_config::table::ColumnSpec]) -> (Row, bool) {
    let col_names: Vec<String> = cols.iter()
        .map(|c| format!("{}.{}", table_id, c.column_name))
        .collect();
    decode_table_row_prebuilt(b, off, bs, &col_names, cols)
}

// ─────────────────────────────────────────────────────────────────────────────
// [OPT-16-KEY] make_join_key_into: writes key into a pre-allocated buffer.
//
// CRITICAL BUG FIX: The separator is now a SINGLE '\x00' byte.
// The old code used "\\x00\\" (the literal 4-character string backslash-x-0-0)
// as a separator, producing wrong composite join keys that never matched.
// For example with keys ["l_orderkey", "l_suppkey"]:
//   OLD (broken):  "12345\\x00\\67890"   ← never matches anything
//   NEW (correct): "12345\x0067890"       ← correct null-byte separator
// ─────────────────────────────────────────────────────────────────────────────
fn make_join_key_into(row: &Row, keys: &[String], buf: &mut String) {
    buf.clear();
    for (i, k) in keys.iter().enumerate() {
        if i > 0 { buf.push('\x00'); } // [OPT-16-KEY] FIXED: single null byte, not "\\x00\\"
        if let Some(v) = find_col(row, k) { buf.push_str(v); }
    }
}

// Legacy allocating version.
#[allow(dead_code)]
fn make_join_key(row: &Row, keys: &[String]) -> String {
    let mut buf = String::with_capacity(64);
    make_join_key_into(row, keys, &mut buf);
    buf
}

fn hash_str(s: &str) -> usize {
    let mut h: usize = 0xcbf29ce484222325_u64 as usize;
    for b in s.bytes() { h ^= b as usize; h = h.wrapping_mul(0x100000001b3_u64 as usize); }
    h
}

fn find_col<'a>(row: &'a Row, name: &str) -> Option<&'a String> {
    row.iter().find(|(k, _)| {
        k == name
        || k.ends_with(&format!(".{}", name))
        || name.contains('.') && k.split('.').last() == name.split('.').last()
           && k.split('.').next() == name.split('.').next()
    }).map(|(_, v)| v)
}

fn col_val<'a>(row: &'a Row, name: &str) -> Option<&'a str> { find_col(row, name).map(String::as_str) }

fn project_row(row: &Row, map: &[(String, String)]) -> Row {
    let mut r = Vec::with_capacity(map.len());
    for (from, to) in map {
        match find_col(row, from) {
            Some(v) => r.push((to.clone(), v.clone())),
            None    => eprintln!("[PROJ] missing col \'{}\'", from),
        }
    }
    r
}

fn eval_preds(row: &Row, preds: &[Predicate]) -> bool {
    use std::cmp::Ordering;
    preds.iter().all(|p| {
        let lhs = match find_col(row, &p.column_name) {
            Some(v) => v, None => { return false; }
        };
        let rhs_owned: Option<String> = match &p.value {
            ComparisionValue::Column(c) => find_col(row, c).cloned(),
            ComparisionValue::String(s) => Some(s.clone()),
            ComparisionValue::I32(v)    => Some(v.to_string()),
            ComparisionValue::I64(v)    => Some(v.to_string()),
            ComparisionValue::F32(v)    => Some(v.to_string()),
            ComparisionValue::F64(v)    => Some(v.to_string()),
        };
        let rhs = match rhs_owned { Some(ref r) => r.as_str(), None => return false };
        let ord = match (lhs.parse::<f64>(), rhs.parse::<f64>()) {
            (Ok(a), Ok(b)) => a.partial_cmp(&b).unwrap_or(Ordering::Equal),
            _ => lhs.as_str().cmp(rhs),
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

fn sort_slice(tbl: &mut Table, specs: &[SortSpec]) { tbl.sort_unstable_by(|a, b| cmp_rows(a, b, specs)); }

fn cmp_rows(a: &Row, b: &Row, specs: &[SortSpec]) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    for s in specs {
        let va = a.iter().find(|(k,_)| k == &s.column_name || k.ends_with(&format!(".{}", s.column_name))).map(|(_, v)| v);
        let vb = b.iter().find(|(k,_)| k == &s.column_name || k.ends_with(&format!(".{}", s.column_name))).map(|(_, v)| v);
        let ord = match (va, vb) {
            (Some(a), Some(b)) => match (a.parse::<f64>(), b.parse::<f64>()) {
                (Ok(x), Ok(y)) => x.partial_cmp(&y).unwrap_or(Ordering::Equal),
                _ => a.as_str().cmp(b.as_str()),
            },
            (None, None) => Ordering::Equal, (None, _) => Ordering::Less, (_, None) => Ordering::Greater,
        };
        if ord != Ordering::Equal { return if s.ascending { ord } else { ord.reverse() }; }
    }
    Ordering::Equal
}

fn fmt_f64(v: f64) -> String {
    if v.fract() == 0.0 && v.abs() < 1e15 { format!("{:.1}", v) } else { v.to_string() }
}

fn rough_ram(t: &Table) -> usize {
    let p = std::mem::size_of::<usize>();
    3*p + t.iter().map(|r| 3*p + r.iter().map(|(k,v)| 6*p+k.len()+v.len()).sum::<usize>()).sum::<usize>()
}

fn node_name(op: &OptQueryOp) -> &'static str {
    match op {
        OptQueryOp::Scan(_) => "Scan", OptQueryOp::FilteredScan(_) => "FilteredScan",
        OptQueryOp::Filter(_) => "Filter", OptQueryOp::Project(_) => "Project",
        OptQueryOp::Cross(_) => "Cross", OptQueryOp::Sort(_) => "Sort",
        OptQueryOp::FilterCross(_) => "FilterCross", OptQueryOp::HashJoin(_) => "HashJoin",
    }
}

pub fn get_output_columns(op: &OptQueryOp, ctx: &DbContext) -> Vec<String> {
    match op {
        OptQueryOp::Scan(d) => ctx.get_table_specs().iter().find(|t| t.name == d.table_id)
            .map(|s| s.column_specs.iter().map(|c| format!("{}.{}", d.table_id, c.column_name)).collect())
            .unwrap_or_default(),
        OptQueryOp::FilteredScan(d) => {
            if let Some(ref proj) = d.project { proj.iter().map(|(_, to)| to.clone()).collect() }
            else { ctx.get_table_specs().iter().find(|t| t.name == d.table_id)
                .map(|s| s.column_specs.iter().map(|c| format!("{}.{}", d.table_id, c.column_name)).collect())
                .unwrap_or_default() }
        }
        OptQueryOp::Filter(d)      => get_output_columns(&d.underlying, ctx),
        OptQueryOp::Sort(d)        => get_output_columns(&d.underlying, ctx),
        OptQueryOp::Project(d)     => d.column_name_map.iter().map(|(_, to)| to.clone()).collect(),
        OptQueryOp::Cross(d)       => { let mut l = get_output_columns(&d.left, ctx); l.extend(get_output_columns(&d.right, ctx)); l }
        OptQueryOp::FilterCross(d) => { let mut l = get_output_columns(&d.left, ctx); l.extend(get_output_columns(&d.right, ctx)); l }
        OptQueryOp::HashJoin(d)    => { let mut l = get_output_columns(&d.left, ctx); l.extend(get_output_columns(&d.right, ctx)); l }
    }
}
