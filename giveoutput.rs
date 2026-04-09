// use anyhow::Result; // handling errors
// use std::collections::HashMap; // row is basically column name to value
// use std::io::{BufRead, Read, Write};

// use common::query::*;
// use common::DataType; // int32 float64 string etc
// use db_config::DbContext;

// use crate::FILE_ID; // schema column type

// type Row = HashMap<String, String>;
// type Table = Vec<Row>;

// pub struct Executor<'a, R: BufRead, W: Write> {
//     disk_in: R,
//     disk_out: &'a mut W,
//     block_size: u64,
//     ctx: &'a DbContext,
// }

// impl<'a, R: BufRead, W: Write> Executor<'a, R, W> {
//     pub fn new(mut disk_in: R, disk_out: &'a mut W, ctx: &'a DbContext) -> Result<Self> {
//         disk_out.write_all(b"get block-size\n")?;
//         disk_out.flush()?;

//         let line = Self::read_line(&mut disk_in)?;
//         let block_size: u64 = line.trim().parse()?;

//         Ok(Self {
//             disk_in,
//             disk_out,
//             block_size,
//             ctx,
//         })
//     }

//     fn read_line(reader: &mut R) -> Result<String> {
//         let mut buf = Vec::new();
//         let mut byte = [0u8; 1];

//         loop {
//             let n = reader.read(&mut byte)?;
//             if n == 0 {
//                 break;
//             }
//             buf.push(byte[0]);
//             if byte[0] == b'\n' {
//                 break;
//             }
//         }

//         Ok(String::from_utf8_lossy(&buf).to_string())
//     }

//     pub fn execute(&mut self, op: &QueryOp) -> Result<Table> {
//         match op {
//             QueryOp::Scan(data) => self.execute_scan(data),
//             QueryOp::Filter(data) => self.execute_filter(data),
//             QueryOp::Project(data) => self.execute_project(data),
//             QueryOp::Cross(data) => self.execute_cross(data),
//             QueryOp::Sort(data) => self.execute_sort(data),
//         }
//     }

//     fn execute_scan(&mut self, data: &ScanData) -> Result<Table> {
//         *FILE_ID.lock().unwrap() = data.table_id.clone();

//         let cmd = format!("get file start-block {}\n", FILE_ID.lock().unwrap());
//         self.disk_out.write_all(cmd.as_bytes())?;
//         self.disk_out.flush()?;
//         let start_block: u64 = Self::read_line(&mut self.disk_in)?.trim().parse()?;

//         let cmd = format!("get file num-blocks {}\n", FILE_ID.lock().unwrap());
//         self.disk_out.write_all(cmd.as_bytes())?;
//         self.disk_out.flush()?;
//         let num_blocks: u64 = Self::read_line(&mut self.disk_in)?.trim().parse()?;

//         let cmd = format!("get block {} {}\n", start_block, num_blocks);
//         self.disk_out.write_all(cmd.as_bytes())?;
//         self.disk_out.flush()?;

//         let total_bytes = num_blocks * self.block_size;
//         let mut buf = vec![0u8; total_bytes as usize];
//         self.disk_in.read_exact(&mut buf)?;

//         let table_spec = self
//             .ctx
//             .get_table_specs()
//             .iter()
//             .find(|t| t.file_id == *FILE_ID.lock().unwrap())
//             .expect("table not found");

//         let mut table = Vec::new();
//         let mut offset = 0usize;

//         while offset < buf.len() {
//             // Stop if remaining buffer is all zeros
//             if buf[offset..].iter().all(|&b| b == 0) {
//                 break;
//             }

//             let mut row: Row = HashMap::new();

//             for col in &table_spec.column_specs {
//                 let value = match col.data_type {
//                     DataType::Int32 => {
//                         if offset + 4 > buf.len() {
//                             return Ok(table);
//                         }
//                         let bytes: [u8; 4] = buf[offset..offset + 4].try_into().unwrap();
//                         offset += 4;
//                         i32::from_le_bytes(bytes).to_string()
//                     }
//                     DataType::Int64 => {
//                         if offset + 8 > buf.len() {
//                             return Ok(table);
//                         }
//                         let bytes: [u8; 8] = buf[offset..offset + 8].try_into().unwrap();
//                         offset += 8;
//                         i64::from_le_bytes(bytes).to_string()
//                     }
//                     DataType::Float32 => {
//                         if offset + 4 > buf.len() {
//                             return Ok(table);
//                         }
//                         let bytes: [u8; 4] = buf[offset..offset + 4].try_into().unwrap();
//                         offset += 4;
//                         f32::from_le_bytes(bytes).to_string()
//                     }
//                     DataType::Float64 => {
//                         if offset + 8 > buf.len() {
//                             return Ok(table);
//                         }
//                         let bytes: [u8; 8] = buf[offset..offset + 8].try_into().unwrap();
//                         offset += 8;
//                         f64::from_le_bytes(bytes).to_string()
//                     }
//                     DataType::String => {
//                         let start = offset;
//                         while offset < buf.len() && buf[offset] != 0 {
//                             offset += 1;
//                         }
//                         let s = String::from_utf8_lossy(&buf[start..offset]).to_string();
//                         if offset < buf.len() && buf[offset] == 0 {
//                             offset += 1;
//                         }
//                         s
//                     }
//                 };

//                 row.insert(col.column_name.clone(), value);
//             }

//             // 🔥 CRITICAL FIX: detect garbage row and STOP
//             if row.values().all(|v| v.is_empty() || v == "0") {
//                 break;
//             }

//             table.push(row);
//         }

//         Ok(table)
//     }

//     fn execute_filter(&mut self, data: &FilterData) -> Result<Table> {
//         let input = self.execute(&data.underlying)?;
//         Ok(input
//             .into_iter()
//             .filter(|row| self.evaluate_predicates(row, &data.predicates))
//             .collect())
//     }

//     fn evaluate_predicates(&self, row: &Row, preds: &[Predicate]) -> bool {
//         preds.iter().all(|p| self.evaluate_predicate(row, p))
//     }

//     fn evaluate_predicate(&self, row: &Row, pred: &Predicate) -> bool {
//         let lhs = match row.get(&pred.column_name) {
//             Some(v) => v,
//             None => return false,
//         };

//         match &pred.value {
//             ComparisionValue::String(rhs) => self.compare(lhs, rhs, &pred.operator),
//             ComparisionValue::I32(v) => self.compare(lhs, &v.to_string(), &pred.operator),
//             ComparisionValue::I64(v) => self.compare(lhs, &v.to_string(), &pred.operator),
//             ComparisionValue::F32(v) => self.compare(lhs, &v.to_string(), &pred.operator),
//             ComparisionValue::F64(v) => self.compare(lhs, &v.to_string(), &pred.operator),
//             ComparisionValue::Column(col) => match row.get(col) {
//                 Some(rhs) => self.compare(lhs, rhs, &pred.operator),
//                 None => false,
//             },
//         }
//     }

//     fn compare(&self, lhs: &str, rhs: &str, op: &ComparisionOperator) -> bool {
//         match op {
//             ComparisionOperator::EQ => lhs == rhs,
//             ComparisionOperator::NE => lhs != rhs,
//             ComparisionOperator::GT => lhs > rhs,
//             ComparisionOperator::GTE => lhs >= rhs,
//             ComparisionOperator::LT => lhs < rhs,
//             ComparisionOperator::LTE => lhs <= rhs,
//         }
//     }

//     fn execute_project(&mut self, data: &ProjectData) -> Result<Table> {
//         let input = self.execute(&data.underlying)?;

//         Ok(input
//             .into_iter()
//             .map(|row| {
//                 let mut new_row = HashMap::new();
//                 for (from, to) in &data.column_name_map {
//                     if let Some(val) = row.get(from) {
//                         new_row.insert(to.clone(), val.clone());
//                     }
//                 }
//                 new_row
//             })
//             .collect())
//     }

//     fn execute_cross(&mut self, data: &CrossData) -> Result<Table> {
//         let left = self.execute(&data.left)?;
//         let right = self.execute(&data.right)?;

//         let mut result = Vec::new();
//         for l in &left {
//             for r in &right {
//                 let mut row = l.clone();
//                 for (k, v) in r {
//                     row.insert(k.clone(), v.clone());
//                 }
//                 result.push(row);
//             }
//         }

//         Ok(result)
//     }

//     fn execute_sort(&mut self, data: &SortData) -> Result<Table> {
//         let mut input = self.execute(&data.underlying)?;

//         input.sort_by(|a, b| {
//             for spec in &data.sort_specs {
//                 let va = a.get(&spec.column_name);
//                 let vb = b.get(&spec.column_name);

//                 if va != vb {
//                     let ord = va.cmp(&vb);
//                     return if spec.ascending { ord } else { ord.reverse() };
//                 }
//             }
//             std::cmp::Ordering::Equal
//         });

//         Ok(input)
//     }
// }
use anyhow::Result;
use std::io::{BufRead, Write};
use common::query::*;
use db_config::DbContext;
use common::DataType;

type Row = Vec<(String, String)>;   // ✅ ORDERED
type Table = Vec<Row>;

pub struct Executor<'a, R: BufRead, W: Write> {
    disk_in: R,
    disk_out: &'a mut W,
    block_size: u64,
    ctx: &'a DbContext,
}

impl<'a, R: BufRead, W: Write> Executor<'a, R, W> {
    pub fn new(mut disk_in: R, disk_out: &'a mut W, ctx: &'a DbContext) -> Result<Self> {
        disk_out.write_all(b"get block-size\n")?;
        disk_out.flush()?;

        let mut line = String::new();
        disk_in.read_line(&mut line)?;
        let block_size: u64 = line.trim().parse()?;

        Ok(Self { disk_in, disk_out, block_size, ctx })
    }

    pub fn execute(&mut self, op: &QueryOp) -> Result<Table> {
        let result = match op {
            QueryOp::Scan(data) => self.execute_scan(data),
            QueryOp::Filter(data) => self.execute_filter(data),
            QueryOp::Project(data) => self.execute_project(data),
            QueryOp::Cross(data) => self.execute_cross(data),
            QueryOp::Sort(data) => self.execute_sort(data),
        }?;

        // ✅ DEBUG PRINT
        println!("\n=== {:?} OUTPUT ({} rows) ===", op, result.len());
        for row in result.iter().take(3) {
            println!("{:?}", row);
        }
        
        if let QueryOp::Filter(filter_data) = op {
            // check it's the specific filter you care about
            if filter_data.predicates.len() == 1 {
                let p = &filter_data.predicates[0];

                if p.column_name == "c_nationkey"
                    && matches!(p.operator, ComparisionOperator::GTE)
                    && matches!(p.value, ComparisionValue::I32(14))
                {
                    println!("=== FULL OUTPUT ===");
                    for row in result.iter().take(321) {
                        println!("{:?}", row);
                    }
                }
            }
        }
        Ok(result)
    }

    // ---------------- SCAN ----------------
    fn execute_scan(&mut self, data: &ScanData) -> Result<Table> {
        let table_spec = self
            .ctx
            .get_table_specs()
            .iter()
            .find(|t| t.name == data.table_id)
            .expect("table not found");

        let file_id = table_spec.file_id.clone();

        let mut line = String::new();

        // --- metadata ---
        self.disk_out.write_all(format!("get file start-block {}\n", file_id).as_bytes())?;
        self.disk_out.flush()?;
        self.disk_in.read_line(&mut line)?;
        let start_block: u64 = line.trim().parse()?;
        line.clear();

        self.disk_out.write_all(format!("get file num-blocks {}\n", file_id).as_bytes())?;
        self.disk_out.flush()?;
        self.disk_in.read_line(&mut line)?;
        let num_blocks: u64 = line.trim().parse()?;
        line.clear();

        self.disk_out.write_all(format!("get block {} {}\n", start_block, num_blocks).as_bytes())?;
        self.disk_out.flush()?;

        let total_bytes = (num_blocks * self.block_size) as usize;
        let mut buf = vec![0u8; total_bytes];

        let mut filled = 0;
        while filled < total_bytes {
            let available = self.disk_in.fill_buf()?;
            if available.is_empty() { break; }

            let to_copy = available.len().min(total_bytes - filled);
            buf[filled..filled + to_copy].copy_from_slice(&available[..to_copy]);
            self.disk_in.consume(to_copy);
            filled += to_copy;
        }

        println!("[SCAN DEBUG] bytes_filled = {}", filled);

        let mut table = Vec::new();
        let mut offset = 0;

        while offset < filled {
            // 🔥 skip padding BEFORE starting row
            // while offset < filled && buf[offset] == 0 {
            //     offset += 1;
            // }

            if offset >= filled {
                break;
            }

            let row_start = offset;
            let mut row: Row = Vec::new();
            let mut local_offset = offset;
            let mut valid_row = true;

            for col in &table_spec.column_specs {
                let value = match col.data_type {
                    DataType::Int32 => {
                        if local_offset + 4 > filled {
                            valid_row = false;
                            break;
                        }
                        let bytes: [u8; 4] = buf[local_offset..local_offset+4].try_into().unwrap();
                        local_offset += 4;

                        let v = i32::from_le_bytes(bytes);
                        v.to_string()   // ✅ allow 0
                    }

                    DataType::Int64 => {
                        if local_offset + 8 > filled {
                            valid_row = false;
                            break;
                        }
                        let bytes: [u8; 8] = buf[local_offset..local_offset+8].try_into().unwrap();
                        local_offset += 8;

                        let v = i64::from_le_bytes(bytes);
                        v.to_string()
                    }

                    DataType::Float64 => {
                        if local_offset + 8 > filled {
                            valid_row = false;
                            break;
                        }
                        let bytes: [u8; 8] = buf[local_offset..local_offset+8].try_into().unwrap();
                        local_offset += 8;

                        let v = f64::from_le_bytes(bytes);

                        // ❗ ONLY reject true corruption
                        if !v.is_finite() {
                            valid_row = false;
                            break;
                        }

                        v.to_string()
                    }

                    DataType::String => {
                        let start = local_offset;

                        while local_offset < filled && buf[local_offset] != 0 {
                            local_offset += 1;
                        }

                        if local_offset >= filled {
                            valid_row = false;
                            break;
                        }

                        let s = String::from_utf8_lossy(&buf[start..local_offset]).to_string();

                        // ❗ reject empty string
                        if s.is_empty() {
                            valid_row = false;
                            break;
                        }

                        local_offset += 1; // skip string null
                        s
                    }

                    _ => panic!("Unsupported type"),
                };

                let key = format!("{}.{}", data.table_id, col.column_name);
                row.push((key, value));
            }

            if !valid_row || row.len() != table_spec.column_specs.len() {
                // println!("[SCAN DEBUG] Skipping malformed row at offset {}", row_start);

                // 🔥 move forward safely
                offset = row_start + 1;
                continue;
            }

            // println!("[SCAN DEBUG] ROW = {:?}", row);
            table.push(row);

            // ✅ move to end of row
            offset = local_offset;

            // 🔥 CRITICAL: skip ALL padding after row
            // while offset < filled && buf[offset] == 0 {
            //     offset += 1;
            // }
        }

        println!("[SCAN DEBUG] TOTAL ROWS = {}", table.len());
        Ok(table)
    }


    // ---------------- FILTER ----------------
    fn execute_filter(&mut self, data: &FilterData) -> Result<Table> {
        let input = self.execute(&data.underlying)?;

        Ok(input
            .into_iter()
            .filter(|row| self.eval_preds(row, &data.predicates))
            .collect())
    }

    fn get_val<'b>(&self, row: &'b Row, col: &String) -> Option<&'b String> {
        row.iter()
            .find(|(k, _)| k == col || k.ends_with(&format!(".{}", col)))
            .map(|(_, v)| v)
    }

    fn eval_preds(&self, row: &Row, preds: &[Predicate]) -> bool {
        use std::cmp::Ordering;

        preds.iter().all(|p| {
            let lhs = match self.get_val(row, &p.column_name) {
                Some(v) => v,
                None => return false,
            };

            // Convert RHS into owned String (safe, no dangling refs)
            let rhs_owned: Option<String> = match &p.value {
                ComparisionValue::Column(c) => self.get_val(row, c).cloned(),
                ComparisionValue::String(s) => Some(s.clone()),
                ComparisionValue::I32(v) => Some(v.to_string()),
                ComparisionValue::I64(v) => Some(v.to_string()),
                ComparisionValue::F32(v) => Some(v.to_string()),
                ComparisionValue::F64(v) => Some(v.to_string()),
            };

            let rhs = match rhs_owned {
                Some(ref r) => r,
                None => return false,
            };

            // 🔥 Try numeric comparison first
            let ord = match (lhs.parse::<f64>(), rhs.parse::<f64>()) {
                (Ok(l), Ok(r)) => l.partial_cmp(&r).unwrap_or(Ordering::Equal),
                _ => lhs.cmp(rhs), // fallback to string comparison
            };

            match p.operator {
                ComparisionOperator::EQ => ord == Ordering::Equal,
                ComparisionOperator::NE => ord != Ordering::Equal,
                ComparisionOperator::GT => ord == Ordering::Greater,
                ComparisionOperator::GTE => ord == Ordering::Greater || ord == Ordering::Equal,
                ComparisionOperator::LT => ord == Ordering::Less,
                ComparisionOperator::LTE => ord == Ordering::Less || ord == Ordering::Equal,
            }
        })
    }

    // ---------------- PROJECT ----------------
    fn execute_project(&mut self, data: &ProjectData) -> Result<Table> {
        let input = self.execute(&data.underlying)?;

        Ok(input.into_iter().map(|row| {
            let mut new_row = Vec::new();

            for (from, to) in &data.column_name_map {
                if let Some(val) = self.get_val(&row, from) {
                    new_row.push((to.clone(), val.clone()));
                }
            }

            new_row
        }).collect())
    }

    // ---------------- CROSS ----------------
    fn execute_cross(&mut self, data: &CrossData) -> Result<Table> {
        let left = self.execute(&data.left)?;
        let right = self.execute(&data.right)?;

        let mut result = Vec::new();

        for l in &left {
            for r in &right {
                let mut row = l.clone();
                row.extend(r.clone());
                result.push(row);
            }
        }

        Ok(result)
    }

    // ---------------- SORT ----------------
    fn execute_sort(&mut self, data: &SortData) -> Result<Table> {
        let mut input = self.execute(&data.underlying)?;

        input.sort_by(|a, b| {
            for spec in &data.sort_specs {
                let va_opt = self.get_val(a, &spec.column_name);
                let vb_opt = self.get_val(b, &spec.column_name);

                let ord = match (va_opt, vb_opt) {
                    (Some(va), Some(vb)) => {
                        // 🔥 try numeric comparison
                        match (va.parse::<i64>(), vb.parse::<i64>()) {
                            (Ok(na), Ok(nb)) => na.cmp(&nb),
                            _ => va.cmp(vb), // fallback string compare
                        }
                    }
                    (None, None) => std::cmp::Ordering::Equal,
                    (None, _) => std::cmp::Ordering::Less,
                    (_, None) => std::cmp::Ordering::Greater,
                };

                if ord != std::cmp::Ordering::Equal {
                    return if spec.ascending { ord } else { ord.reverse() };
                }
            }

            std::cmp::Ordering::Equal
        });

        Ok(input)
    }
}

// ---------------- OUTPUT ----------------
pub fn print_output<W: Write>(
    out: &mut W,
    table: &Table,
    columns: Vec<String>,
) -> Result<()> {
    for row in table {
        for col in &columns {
            let val = row.iter()
                .find(|(k, _)| k == col)
                .map(|(_, v)| v.clone())
                .unwrap_or_default();

            write!(out, "{}|", val)?;
        }
        writeln!(out)?;
    }
    Ok(())
}

pub fn get_output_columns(op: &QueryOp, ctx: &DbContext) -> Vec<String> {
    match op {
        QueryOp::Scan(data) => {
            ctx.get_table_specs()
                .iter()
                .find(|t| t.name == data.table_id)
                .map(|spec| {
                    spec.column_specs
                        .iter()
                        .map(|c| format!("{}.{}", data.table_id, c.column_name))
                        .collect()
                })
                .unwrap_or_default()
        }
        QueryOp::Filter(data) => get_output_columns(&data.underlying, ctx),
        QueryOp::Sort(data) => get_output_columns(&data.underlying, ctx),
        QueryOp::Project(data) => {
            data.column_name_map
                .iter()
                .map(|(_, to)| to.clone())
                .collect()
        }
        QueryOp::Cross(data) => {
            let mut left = get_output_columns(&data.left, ctx);
            let right = get_output_columns(&data.right, ctx);
            left.extend(right);
            left
        }
    }
}