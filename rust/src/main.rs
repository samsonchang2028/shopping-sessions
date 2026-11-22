//! Shopping Sessions, native engine. Spec: ../rules.md.
//!
//! One pass over the mmapped CSV bytes with our own exact line grammar (every comma splits, every '\n'
//! ends a line, quotes only around a whole field), then everything runs on integer keys:
//!   parse -> dedup by hashed id (exact on collision) -> intern user IDs to byte-sorted indexes ->
//!   union-find (min index root) -> one parallel sort of packed (person, time) keys -> sessions ->
//!   credit by two-pointer merge -> assists via per-(person, campaign) click islands -> Parquet.
//! Exit codes: 0 ok, 3 = input outside what this engine supports (caller falls back to the DuckDB engine).

use std::collections::{HashMap, HashSet};
use std::hash::BuildHasher;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use arrow_array::builder::{Date32Builder, Int64Builder, StringBuilder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use foldhash::fast::FixedState;
use memchr::{memchr, memchr_iter};
use memmap2::Mmap;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rayon::prelude::*;

#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

const DAY: i64 = 86_400_000;
const GAP: i64 = 1_800_000;
const WEEK: i64 = 604_800_000;
const NB: usize = 256; // dedup buckets
const CHUNK: usize = 8 << 20;
const DECLINE: i32 = 3;

type FMap<K, V> = HashMap<K, V, FixedState>;
type FSet<K> = HashSet<K, FixedState>;

struct Logger(Instant);
/// User+system CPU seconds of this process so far (from /proc/self/stat, 100 ticks per second).
fn cpu_secs() -> f64 {
    let st = std::fs::read_to_string("/proc/self/stat").unwrap_or_default();
    let rest = st.rsplit(')').next().unwrap_or("");
    let f: Vec<&str> = rest.split_whitespace().collect();
    let t = |i: usize| f.get(i).and_then(|x| x.parse::<f64>().ok()).unwrap_or(0.0);
    (t(11) + t(12)) / 100.0
}
impl Logger {
    fn log(&self, what: &str) {
        eprintln!("[{:7.3}s cpu {:6.2}] {}", self.0.elapsed().as_secs_f64(), cpu_secs(), what);
    }
}

fn decline(why: &str) -> ! {
    eprintln!("ss: declining: {why}");
    std::process::exit(DECLINE);
}

// ---------------------------------------------------------------- bytes helpers

/// A byte slice packed into 8 bytes: 48-bit address << 16 | 16-bit length. Every slice points into an Mmap
/// that lives until process exit. A field of 64 KiB or more is outside what this engine supports.
#[derive(Clone, Copy)]
struct Raw {
    v: u64,
}
impl Raw {
    #[inline]
    fn of(s: &[u8]) -> Raw {
        if s.len() > 0xffff {
            decline("field longer than 64 KiB");
        }
        Raw { v: ((s.as_ptr() as u64) << 16) | s.len() as u64 }
    }
    #[inline]
    fn get<'a>(self) -> &'a [u8] {
        // SAFETY: created by `of` from a live mmap slice.
        unsafe { std::slice::from_raw_parts((self.v >> 16) as *const u8, (self.v & 0xffff) as usize) }
    }
}

/// Hasher for keys that are already hashes.
#[derive(Default, Clone, Copy)]
struct IdHasher(u64);
impl std::hash::Hasher for IdHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, b: &[u8]) {
        for &x in b {
            self.0 = self.0.rotate_left(8) ^ x as u64;
        }
    }
    fn write_u64(&mut self, x: u64) {
        self.0 = x;
    }
}
type IdMap<V> = HashMap<u64, V, std::hash::BuildHasherDefault<IdHasher>>;

/// (64-bit hash, independent 32-bit check) of an ID. Two different IDs sharing the 64-bit hash but not the
/// check make the engine decline; sharing both (about 2^-96 per pair) is treated as the same ID.
#[inline]
fn uhash(b: &[u8]) -> (u64, u32) {
    let h = hash_bytes(b);
    (h | (h == 0) as u64, (FixedState::with_seed(0x517c_c1b7_2722_0a95).hash_one(b) >> 32) as u32)
}

#[inline]
fn hash_bytes(b: &[u8]) -> u64 {
    FixedState::default().hash_one(b)
}

/// One CSV field under rules.md: no quote at all, or one quote pair around the whole field.
/// Returns the unquoted bytes (empty means null), or None if the field is malformed.
#[inline]
fn field(f: &[u8]) -> Option<&[u8]> {
    if memchr(b'"', f).is_none() {
        return Some(f);
    }
    let n = f.len();
    if n >= 2 && f[0] == b'"' && f[n - 1] == b'"' && memchr(b'"', &f[1..n - 1]).is_none() {
        Some(&f[1..n - 1])
    } else {
        None
    }
}

/// Split a line into exactly N well-formed fields (unquoted; empty means null).
/// Quote rule without per-field scans: a field is "quoted" if it starts with a quote; it must also end with one
/// (length >= 2). The line is well formed iff its total quote count is exactly 2 per quoted field: every quoted
/// field then has no inner quote and every other field has none at all.
#[inline]
fn split<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    let mut out: [&[u8]; N] = [&line[0..0]; N];
    let mut s = 0;
    let mut i = 0;
    let mut quoted = 0usize;
    for p in memchr_iter(b',', line) {
        if i == N - 1 {
            return None;
        }
        let f = &line[s..p];
        out[i] = if !f.is_empty() && f[0] == b'"' {
            if f.len() < 2 || f[f.len() - 1] != b'"' {
                return None;
            }
            quoted += 1;
            &f[1..f.len() - 1]
        } else {
            f
        };
        i += 1;
        s = p + 1;
    }
    if i != N - 1 {
        return None;
    }
    let f = &line[s..];
    out[i] = if !f.is_empty() && f[0] == b'"' {
        if f.len() < 2 || f[f.len() - 1] != b'"' {
            return None;
        }
        quoted += 1;
        &f[1..f.len() - 1]
    } else {
        f
    };
    let q = line.iter().filter(|&&c| c == b'"').count();
    if q != 2 * quoted {
        return None;
    }
    Some(out)
}

/// Scan every line of `b` (a run of whole lines) and call `f(line, fields)` for each line that is well formed under
/// the dialect: exactly N fields, quotes only around whole fields. Fields come unquoted; empty means null.
///
/// x86-64: one SSE2 pass builds comma / newline / quote bitmasks for each 64-byte block and walks the set bits, so
/// a line costs a few bit operations instead of one memchr call per field. A line's quote check is a popcount
/// difference: its total quote count must be exactly 2 per field that starts with a quote.
#[cfg(target_arch = "x86_64")]
#[inline]
fn scan<const N: usize>(b: &[u8], mut f: impl FnMut(&[u8], [&[u8]; N])) {
    use std::arch::x86_64::*;
    let len = b.len();
    let mut line_start = 0usize;
    let mut q_start = 0u64;
    let mut qcum = 0u64; // quote bytes in all blocks before the current one
    let mut ends = [0usize; N];
    let mut ncomma = 0usize;
    let mut tail = [0u8; 64];
    let mut o = 0usize;
    // Finish a line ending at absolute position `pos` with `q_end` quote bytes before it.
    macro_rules! finish {
        ($pos:expr, $q_end:expr) => {{
            let pos: usize = $pos;
            if ncomma == N - 1 {
                let mut out: [&[u8]; N] = [&b[0..0]; N];
                let mut quoted = 0u64;
                let mut ok = true;
                let mut st = line_start;
                for i in 0..N {
                    let en = if i < N - 1 { ends[i] } else { pos };
                    let fl = &b[st..en];
                    out[i] = if !fl.is_empty() && fl[0] == b'"' {
                        if fl.len() < 2 || fl[fl.len() - 1] != b'"' {
                            ok = false;
                            break;
                        }
                        quoted += 1;
                        &fl[1..fl.len() - 1]
                    } else {
                        fl
                    };
                    st = en + 1;
                }
                if ok && ($q_end) - q_start == 2 * quoted {
                    f(&b[line_start..pos], out);
                }
            }
            line_start = pos + 1;
            q_start = $q_end;
            ncomma = 0;
        }};
    }
    while o < len {
        let (mc, mn, mq) = unsafe {
            let (pc, valid) = if o + 64 <= len {
                (b.as_ptr().add(o), 64)
            } else {
                let n = len - o;
                tail[..n].copy_from_slice(&b[o..]);
                tail[n..].fill(0);
                (tail.as_ptr(), n)
            };
            let _ = valid;
            let (vc, vn, vq) = (_mm_set1_epi8(b',' as i8), _mm_set1_epi8(b'\n' as i8), _mm_set1_epi8(b'"' as i8));
            let (mut mc, mut mn, mut mq) = (0u64, 0u64, 0u64);
            for i in 0..4 {
                let v = _mm_loadu_si128(pc.add(16 * i) as *const __m128i);
                mc |= (_mm_movemask_epi8(_mm_cmpeq_epi8(v, vc)) as u16 as u64) << (16 * i);
                mn |= (_mm_movemask_epi8(_mm_cmpeq_epi8(v, vn)) as u16 as u64) << (16 * i);
                mq |= (_mm_movemask_epi8(_mm_cmpeq_epi8(v, vq)) as u16 as u64) << (16 * i);
            }
            (mc, mn, mq)
        };
        let mut m = mc | mn;
        while m != 0 {
            let t = m.trailing_zeros();
            m &= m - 1;
            let pos = o + t as usize;
            if (mn >> t) & 1 == 1 {
                let q_end = qcum + (mq & ((1u64 << t) - 1)).count_ones() as u64;
                finish!(pos, q_end);
            } else {
                if ncomma < N - 1 {
                    ends[ncomma] = pos;
                }
                ncomma += 1;
            }
        }
        qcum += mq.count_ones() as u64;
        o += 64;
    }
    if line_start < len {
        finish!(len, qcum);
    }
}

#[cfg(not(target_arch = "x86_64"))]
fn scan<const N: usize>(b: &[u8], mut f: impl FnMut(&[u8], [&[u8]; N])) {
    for_lines(b, |line| {
        if let Some(fl) = split::<N>(line) {
            f(line, fl);
        }
    });
}

fn for_lines(b: &[u8], mut f: impl FnMut(&[u8])) {
    let mut s = 0;
    while s < b.len() {
        match memchr(b'\n', &b[s..]) {
            Some(p) => {
                f(&b[s..s + p]);
                s += p + 1;
            }
            None => {
                f(&b[s..]);
                break;
            }
        }
    }
}

// ---------------------------------------------------------------- timestamps

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[inline]
fn d2(s: &[u8], i: usize) -> Option<i64> {
    let (a, b) = (s[i].wrapping_sub(b'0'), s[i + 1].wrapping_sub(b'0'));
    if a < 10 && b < 10 {
        Some((a * 10 + b) as i64)
    } else {
        None
    }
}

/// Days since the epoch of an exact "YYYY-MM-DD" (10 bytes), None if it is not a real calendar date.
fn date_days(s: &[u8]) -> Option<i64> {
    if s[4] != b'-' || s[7] != b'-' {
        return None;
    }
    let y = d2(s, 0)? * 100 + d2(s, 2)?;
    let (mo, d) = (d2(s, 5)?, d2(s, 8)?);
    if !(1..=12).contains(&mo) || d < 1 {
        return None;
    }
    let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
    let dim = match mo {
        2 => 28 + leap as i64,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    if d > dim {
        return None;
    }
    Some(days_from_civil(y, mo, d))
}

/// True iff every byte selected by `mask` (0xFF bytes) of `x` is an ASCII digit '0'..='9'. Unselected bytes are ignored.
/// A byte b is a digit iff (b & 0xF0) == 0x30 and ((b + 6) & 0xF0) == 0x30; a byte >= 0xFA that carries into its
/// neighbour is itself rejected, so a carry can never turn a bad input into a good one.
#[inline]
fn swar_digits(x: u64, mask: u64) -> bool {
    let t = x & mask;
    let lo = (t & 0xF0F0_F0F0_F0F0_F0F0) == (0x3030_3030_3030_3030 & mask);
    let hi = (t.wrapping_add(0x0606_0606_0606_0606 & mask) & 0xF0F0_F0F0_F0F0_F0F0) == (0x3030_3030_3030_3030 & mask);
    lo && hi
}

/// Value of 8 ASCII digits (little-endian load, first digit in the low byte). Digits must already be validated.
#[inline]
fn swar_8(x: u64) -> u64 {
    let mut v = x.wrapping_sub(0x3030_3030_3030_3030);
    v = (v.wrapping_mul(10) + (v >> 8)) & 0x00FF_00FF_00FF_00FF;
    v = (v.wrapping_mul(100) + (v >> 16)) & 0x0000_FFFF_0000_FFFF;
    (v.wrapping_mul(10000) + (v >> 32)) & 0xFFFF_FFFF
}

/// Per-thread memo of date prefix -> day number. Every entry is computed by `date_days` from the exact 10 bytes
/// (invalid dates are cached as invalid), so the cache can only repeat what the exact function says.
struct DateCache {
    ent: [(u64, u16, i64); 128], // (first 8 bytes, next 2 bytes, days or i64::MIN for "not a date"); (0,0,_) = empty
}
impl DateCache {
    fn new() -> DateCache {
        DateCache { ent: [(0, 0, 0); 128] }
    }
    #[inline]
    fn days(&mut self, s: &[u8]) -> Option<i64> {
        let a = u64::from_le_bytes(s[0..8].try_into().unwrap());
        let b = u16::from_le_bytes(s[8..10].try_into().unwrap());
        let slot = ((a.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 57) as usize) & 127;
        let e = &mut self.ent[slot];
        if e.0 == a && e.1 == b {
            return if e.2 == i64::MIN { None } else { Some(e.2) };
        }
        let r = date_days(s);
        *e = (a, b, r.unwrap_or(i64::MIN));
        r
    }
}

/// rules.md "Timestamps": F1, F2, F3 to epoch ms UTC, or None. Digits are ASCII only.
#[inline]
fn parse_ts(s: &[u8], dc: &mut DateCache) -> Option<i64> {
    let n = s.len();
    if (n == 20 || n == 25) && s[4] == b'-' {
        let days = dc.days(s)?;
        if s[10] != b'T' {
            return None;
        }
        // "HH:MM:SS" as one 8-byte load: digits at bytes 0,1,3,4,6,7 and ':' at 2 and 5
        let x = u64::from_le_bytes(s[11..19].try_into().unwrap());
        if !swar_digits(x, 0xFFFF_00FF_FF00_FFFF) || (x >> 16) as u8 != b':' || (x >> 40) as u8 != b':' {
            return None;
        }
        let dg = |i: u32| ((x >> (8 * i)) as u8 & 0x0F) as i64;
        let (h, mi, se) = (dg(0) * 10 + dg(1), dg(3) * 10 + dg(4), dg(6) * 10 + dg(7));
        if h > 23 || mi > 59 || se > 59 {
            return None;
        }
        let off = if n == 20 {
            if s[19] != b'Z' {
                return None;
            }
            0
        } else {
            let sign = match s[19] {
                b'+' => 1,
                b'-' => -1,
                _ => return None,
            };
            if s[22] != b':' {
                return None;
            }
            let (oh, om) = (d2(s, 20)?, d2(s, 23)?);
            if oh > 23 || om > 59 {
                return None;
            }
            sign * (oh * 3_600_000 + om * 60_000)
        };
        return Some((((days * 24 + h) * 60 + mi) * 60 + se) * 1000 - off);
    }
    // F3: ASCII digits only, at most 15 significant digits (more is outside any window), leading zeros fine.
    if n == 0 {
        return None;
    }
    if n <= 15 {
        // at most 15 digits can never exceed 15 significant digits: 8 digits by SWAR, the rest one by one
        let mut v: u64 = 0;
        let mut i = 0;
        if n >= 8 {
            let x = u64::from_le_bytes(s[0..8].try_into().unwrap());
            if !swar_digits(x, u64::MAX) {
                return None;
            }
            v = swar_8(x);
            i = 8;
        }
        while i < n {
            let d = s[i].wrapping_sub(b'0');
            if d > 9 {
                return None;
            }
            v = v * 10 + d as u64;
            i += 1;
        }
        return Some(v as i64);
    }
    let mut v: i64 = 0;
    let mut sig = 0;
    for &c in s {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        if v > 0 || d > 0 {
            sig += 1;
            if sig > 15 {
                return None;
            }
        }
        v = v * 10 + d as i64;
    }
    Some(v)
}

/// purchase price: ASCII digits, value 1..=10,000,000 (leading zeros fine).
fn parse_price(s: &[u8]) -> Option<u32> {
    if s.is_empty() || !s.iter().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let mut i = 0;
    while i < s.len() && s[i] == b'0' {
        i += 1;
    }
    if s.len() - i > 8 {
        return None;
    }
    let mut v: u64 = 0;
    for &c in &s[i..] {
        v = v * 10 + (c - b'0') as u64;
    }
    if (1..=10_000_000).contains(&v) {
        Some(v as u32)
    } else {
        None
    }
}

// ---------------------------------------------------------------- files

fn csv_files(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map_or(false, |x| x == "csv") && p.is_file())
            .collect(),
        Err(_) => vec![],
    };
    v.sort();
    v
}

fn map_files(dir: &Path) -> Vec<Mmap> {
    csv_files(dir)
        .iter()
        .filter_map(|p| {
            let f = std::fs::File::open(p).ok()?;
            if f.metadata().ok()?.len() == 0 {
                return None; // no header, no rows
            }
            unsafe { Mmap::map(&f).ok() }
        })
        .collect()
}

struct Chunk {
    file: usize,
    start: usize,
    end: usize,
}

/// Split every file (after its header line) into pieces that start and end on line boundaries.
fn chunks(files: &[Mmap]) -> Vec<Chunk> {
    let mut out = vec![];
    for (fi, m) in files.iter().enumerate() {
        let b: &[u8] = m;
        let Some(h) = memchr(b'\n', b) else { continue }; // header only, no newline
        let mut s = h + 1;
        while s < b.len() {
            let mut e = (s + CHUNK).min(b.len());
            if e < b.len() {
                e = match memchr(b'\n', &b[e..]) {
                    Some(p) => e + p + 1,
                    None => b.len(),
                };
            }
            out.push(Chunk { file: fi, start: s, end: e });
            s = e;
        }
    }
    out
}

// ---------------------------------------------------------------- records

/// Event record, 32 bytes. `line` locates the source line; the id is re-derived from it on a hash match.
/// `ts`: bit 31 = valid, low 31 bits = ms since window_start. `pc`: price (24 bits, 1..=10,000,000) in the low bits and
/// the category (8 bits) in the top byte for a valid purchase, else 0 (a valid price is never 0, so `pc != 0` = purchase).
#[derive(Clone, Copy)]
struct Ev {
    h: u64,
    uh: u64,
    line: Raw,
    ts: u32,
    ing: u32, // ingest_ts in whole seconds since the epoch; u32::MAX if invalid
    pc: u32,
}

/// Click record, 40 bytes. `camp == u32::MAX` marks an invalid click.
#[derive(Clone, Copy)]
struct Ck {
    h: u64,
    uh: u64,
    line: Raw,
    ts: u32, // ms since window_start - 7 days
    uk: u32,
    camp: u32,
}

/// The n-th (0-based) field of a validated line, unquoted.
fn nth_field(line: &[u8], n: usize) -> &[u8] {
    let mut it = memchr_iter(b',', line);
    let mut start = 0;
    for _ in 0..n {
        start = it.next().map_or(line.len(), |p| p + 1);
    }
    let end = it.next().unwrap_or(line.len());
    field(&line[start.min(end)..end]).unwrap_or(&line[0..0])
}
#[inline]
fn id_of<'a>(line: Raw) -> &'a [u8] {
    nth_field(line.get(), 0)
}
#[inline]
fn user_of<'a>(line: Raw) -> &'a [u8] {
    nth_field(line.get(), 1)
}

/// Keep, for every distinct id, the first record of the group sorted by (hash, ing). `idof` gives the id bytes.
/// Records with equal hash but different ids (a hash collision) are separated exactly.
fn dedup_sorted<T: Copy>(v: &[T], h: impl Fn(&T) -> u64, idof: impl Fn(&T) -> &[u8], mut keep: impl FnMut(T)) {
    let mut i = 0;
    while i < v.len() {
        let mut j = i + 1;
        while j < v.len() && h(&v[j]) == h(&v[i]) {
            j += 1;
        }
        if j == i + 1 {
            keep(v[i]);
        } else {
            let first = idof(&v[i]);
            if v[i + 1..j].iter().all(|x| idof(x) == first) {
                keep(v[i]);
            } else {
                // collision between different ids: exact grouping, first (smallest) record per id
                let mut seen: HashSet<&[u8]> = HashSet::new();
                for x in &v[i..j] {
                    if seen.insert(idof(x)) {
                        keep(*x);
                    }
                }
            }
        }
        i = j;
    }
}

/// Dedup bucket `b` of every chunk: one record per distinct id, the `earlier` one wins. A hash table keyed on the id
/// hash does it in one pass; if two different ids ever share a hash, the bucket is redone exactly by sorting.
fn dedup_bucket<T: Copy + Send + Sync>(
    parts: &[Vec<Vec<T>>],
    b: usize,
    h: impl Fn(&T) -> u64,
    idof: impl Fn(&T) -> &[u8],
    earlier: impl Fn(&T, &T) -> bool,
    order: impl Fn(&T) -> (u64, u64),
) -> Vec<T> {
    let total: usize = parts.iter().map(|p| p[b].len()).sum();
    let mut map: IdMap<u32> = IdMap::with_capacity_and_hasher(total, Default::default());
    let mut store: Vec<T> = Vec::with_capacity(total);
    let mut collided = false;
    'outer: for p in parts {
        for &e in &p[b] {
            match map.entry(h(&e)) {
                std::collections::hash_map::Entry::Vacant(v) => {
                    v.insert(store.len() as u32);
                    store.push(e);
                }
                std::collections::hash_map::Entry::Occupied(o) => {
                    let cur = &mut store[*o.get() as usize];
                    if idof(cur) != idof(&e) {
                        collided = true;
                        break 'outer;
                    }
                    if earlier(&e, cur) {
                        *cur = e;
                    }
                }
            }
        }
    }
    if !collided {
        return store;
    }
    let mut v: Vec<T> = Vec::with_capacity(total);
    for p in parts {
        v.extend_from_slice(&p[b]);
    }
    v.sort_unstable_by_key(|e| order(e));
    let mut out = Vec::new();
    dedup_sorted(&v, &h, &idof, |e| out.push(e));
    out
}

/// Components of the link graph. Returns, for every linked id whose component's smallest id (by bytes) is another id,
/// the hash of that smallest id. Ids that name their own component are absent.
fn link_components(links: &[(u64, u32, Raw, u64, u32, Raw)]) -> IdMap<u64> {
    let mut idx: IdMap<u32> = IdMap::with_capacity_and_hasher(links.len() * 2, Default::default());
    let mut nodes: Vec<(u64, Raw)> = Vec::with_capacity(links.len() * 2);
    let mut edges: Vec<(u32, u32)> = Vec::with_capacity(links.len());
    for &(ha, _, a, hb, _, b) in links {
        let mut node = |uh: u64, r: Raw| -> u32 {
            *idx.entry(uh).or_insert_with(|| {
                nodes.push((uh, r));
                (nodes.len() - 1) as u32
            })
        };
        let (ia, ib) = (node(ha, a), node(hb, b));
        edges.push((ia, ib));
    }
    let m = nodes.len();
    let mut parent: Vec<u32> = (0..m as u32).collect();
    let mut best: Vec<u32> = (0..m as u32).collect(); // per root: node with the smallest id bytes
    for &(ia, ib) in &edges {
        let (ra, rb) = (find(&mut parent, ia), find(&mut parent, ib));
        if ra != rb {
            if nodes[best[rb as usize] as usize].1.get() < nodes[best[ra as usize] as usize].1.get() {
                best[ra as usize] = best[rb as usize];
            }
            parent[rb as usize] = ra;
        }
    }
    let mut out: IdMap<u64> = IdMap::default();
    for i in 0..m as u32 {
        let r = find(&mut parent, i);
        let name = nodes[best[r as usize] as usize].0;
        if name != nodes[i as usize].0 {
            out.insert(nodes[i as usize].0, name);
        }
    }
    out
}

fn find(parent: &mut [u32], mut x: u32) -> u32 {
    let mut r = x;
    while parent[r as usize] != r {
        r = parent[r as usize];
    }
    while parent[x as usize] != r {
        let n = parent[x as usize];
        parent[x as usize] = r;
        x = n;
    }
    r
}

// ---------------------------------------------------------------- aggregation state

#[derive(Clone, Copy)]
struct E {
    key: u64, // person << 32 | (ts - ws)
    price: u32,
    cat: u16,
    purchase: bool,
}

#[derive(Clone, Copy)]
struct Sess {
    pk: u32,
    start: i64,
    day: u16, // index from the window's first day
    n: u32,
    rev: u64,
    has_p: bool,
}

#[derive(Clone, Copy)]
struct C {
    key: u64, // person << 32 | (ts - (ws - WEEK))
    camp: u32,
    id: Raw,
}

struct UserRow {
    pk: u32,
    day: u16,
    sessions: u32,
    events: u64,
    rev: u64,
    top: u16, // u16::MAX = null
}

struct Acc {
    sess: Vec<u32>,
    purch: Vec<u32>,
    rev: Vec<u64>,
    asst: Vec<u32>,
}
impl Acc {
    fn new(n: usize) -> Acc {
        Acc { sess: vec![0; n], purch: vec![0; n], rev: vec![0; n], asst: vec![0; n] }
    }
    fn add_assign(&mut self, o: &Acc) {
        for i in 0..self.sess.len() {
            self.sess[i] += o.sess[i];
            self.purch[i] += o.purch[i];
            self.rev[i] += o.rev[i];
            self.asst[i] += o.asst[i];
        }
    }
}

fn json_str<'a>(j: &'a str, key: &str) -> Option<&'a str> {
    let k = format!("\"{key}\"");
    let i = j.find(&k)? + k.len();
    let rest = &j[i..];
    let a = rest.find('"')? + 1;
    let b = rest[a..].find('"')?;
    Some(&rest[a..a + b])
}

/// Flat open-addressing table from an ID hash to its person key, built once and then only read. Lookups are issued
/// in batches with a prefetch so their memory round trips overlap (this table is far larger than the L3 cache).
struct PkTable {
    shift: u32,
    mask: usize,
    ents: Vec<(std::sync::atomic::AtomicU64, std::sync::atomic::AtomicU32)>,
}
impl PkTable {
    fn new(n: usize) -> PkTable {
        let cap = (n * 2).max(16).next_power_of_two();
        let ents = (0..cap)
            .into_par_iter()
            .map(|_| (std::sync::atomic::AtomicU64::new(0), std::sync::atomic::AtomicU32::new(0)))
            .collect();
        PkTable { shift: 64 - cap.trailing_zeros(), mask: cap - 1, ents }
    }
    #[inline]
    fn slot(&self, uh: u64) -> usize {
        (uh >> self.shift) as usize
    }
    fn insert(&self, uh: u64, pk: u32) {
        use std::sync::atomic::Ordering::Relaxed;
        let mut i = self.slot(uh);
        loop {
            match self.ents[i].0.compare_exchange(0, uh, Relaxed, Relaxed) {
                Ok(_) => {
                    self.ents[i].1.store(pk, Relaxed);
                    return;
                }
                Err(cur) if cur == uh => return,
                Err(_) => i = (i + 1) & self.mask,
            }
        }
    }
    #[inline]
    fn prefetch(&self, uh: u64) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            std::arch::x86_64::_mm_prefetch(
                self.ents.as_ptr().add(self.slot(uh)) as *const i8,
                std::arch::x86_64::_MM_HINT_T0,
            );
        }
    }
    #[inline]
    fn get(&self, uh: u64) -> u32 {
        use std::sync::atomic::Ordering::Relaxed;
        let mut i = self.slot(uh);
        loop {
            if self.ents[i].0.load(Relaxed) == uh {
                return self.ents[i].1.load(Relaxed);
            }
            i = (i + 1) & self.mask;
        }
    }
}

/// Per-thread scratch plus results for the bucketed per-person pipeline.
struct Worker {
    acc: Acc,
    active: Vec<(i64, i64)>,
    touched: Vec<u32>,
    evs: Vec<E>,
    cks: Vec<C>,
    sess: Vec<Sess>,
    catrev: Vec<u64>,
    ctouched: Vec<u16>,
    urows: Vec<UserRow>,
}
impl Worker {
    fn new(cells: usize, ncamp: usize, ncat: usize) -> Worker {
        Worker {
            acc: Acc::new(cells),
            active: vec![(i64::MIN, i64::MIN); ncamp],
            touched: vec![],
            evs: vec![],
            cks: vec![],
            sess: vec![],
            catrev: vec![0; ncat],
            ctouched: vec![],
            urows: vec![],
        }
    }
    fn merge(mut self, o: Worker) -> Worker {
        self.acc.add_assign(&o.acc);
        self.urows.extend(o.urows);
        self
    }
}

struct Geometry {
    ws: i64,
    ws_day: i64,
    cws: i64,
    ncamp: usize,
    ndays: usize,
}

/// Everything for the persons of one bucket (a range of person keys): sort, sessions, user_daily rows,
/// credit and assists. Buckets hold whole persons, so they are independent.
fn run_bucket(w: &mut Worker, b: usize, evp: &[Vec<Vec<E>>], ckp: &[Vec<Vec<C>>], g: &Geometry) {
    let Worker { acc, active, touched, evs, cks, sess, catrev, ctouched, urows } = w;
    evs.clear();
    for p in evp {
        evs.extend_from_slice(&p[b]);
    }
    cks.clear();
    for p in ckp {
        cks.extend_from_slice(&p[b]);
    }
    evs.sort_unstable_by_key(|e| e.key);
    cks.sort_unstable_by(|a, b| a.key.cmp(&b.key).then_with(|| id_of(a.id).cmp(id_of(b.id))));

    // ---- sessions and user_daily rows
    sess.clear();
    let mut cur: Option<(u32, u16)> = None; // (person, day) group
    let (mut g_sessions, mut g_events, mut g_rev) = (0u32, 0u64, 0u64);
    let mut prev_key = 0u64;
    let mut open = false;
    macro_rules! flush {
        () => {
            if let Some((p, d)) = cur {
                let mut top = u16::MAX;
                let mut best = 0u64;
                for &c in ctouched.iter() {
                    let r = catrev[c as usize];
                    if r > best || (r == best && r > 0 && c < top) {
                        best = r;
                        top = c;
                    }
                    catrev[c as usize] = 0;
                }
                ctouched.clear();
                urows.push(UserRow { pk: p, day: d, sessions: g_sessions, events: g_events, rev: g_rev, top });
                g_sessions = 0;
                g_events = 0;
                g_rev = 0;
            }
        };
    }
    for e in evs.iter() {
        let p = (e.key >> 32) as u32;
        let new = !open
            || (e.key >> 32) != (prev_key >> 32)
            || (e.key & 0xffff_ffff) - (prev_key & 0xffff_ffff) > GAP as u64;
        if new {
            let start = g.ws + (e.key & 0xffff_ffff) as i64;
            let day = (start.div_euclid(DAY) - g.ws_day) as u16;
            if cur != Some((p, day)) {
                flush!();
                cur = Some((p, day));
            }
            g_sessions += 1;
            sess.push(Sess { pk: p, start, day, n: 0, rev: 0, has_p: false });
            open = true;
        }
        prev_key = e.key;
        let s = sess.last_mut().unwrap();
        s.n += 1;
        g_events += 1;
        if e.purchase {
            s.rev += e.price as u64;
            s.has_p = true;
            g_rev += e.price as u64;
            if catrev[e.cat as usize] == 0 {
                ctouched.push(e.cat);
            }
            catrev[e.cat as usize] += e.price as u64;
        }
    }
    flush!();

    // ---- credit and assists, person by person
    let (ncamp, ndays, cws) = (g.ncamp, g.ndays, g.cws);
    let (mut si, mut ci) = (0usize, 0usize);
    while si < sess.len() {
        let p = sess[si].pk;
        let mut sj = si;
        while sj < sess.len() && sess[sj].pk == p {
            sj += 1;
        }
        while ci < cks.len() && ((cks[ci].key >> 32) as u32) < p {
            ci += 1;
        }
        let mut cj = ci;
        while cj < cks.len() && ((cks[cj].key >> 32) as u32) == p {
            cj += 1;
        }
        let ss = &sess[si..sj];
        let cs = &cks[ci..cj];
        // credit: latest click at or before the start; ties keep the first (smallest click_id)
        let mut c = 0usize;
        let mut cand: Option<(i64, u32)> = None;
        for s in ss {
            while c < cs.len() && cws + (cs[c].key & 0xffff_ffff) as i64 <= s.start {
                let t = cws + (cs[c].key & 0xffff_ffff) as i64;
                if cand.map_or(true, |(ct, _)| t > ct) {
                    cand = Some((t, cs[c].camp));
                }
                c += 1;
            }
            let camp = match cand {
                Some((t, camp)) if s.start - t <= WEEK => camp as usize,
                _ => ncamp,
            };
            let cell = camp * ndays + s.day as usize;
            acc.sess[cell] += 1;
            acc.purch[cell] += s.has_p as u32;
            acc.rev[cell] += s.rev;
        }
        // assists: islands of clicks per campaign, each covering session starts in [first, last + 7d]
        let emit = |camp: usize, first: i64, last: i64, acc: &mut Acc| {
            let lo = ss.partition_point(|s| s.start < first);
            let hi = ss.partition_point(|s| s.start <= last + WEEK);
            for s in &ss[lo.min(hi)..hi] {
                acc.asst[camp * ndays + s.day as usize] += 1;
            }
        };
        for k in cs {
            let t = cws + (k.key & 0xffff_ffff) as i64;
            let cm = k.camp as usize;
            let (f, l) = active[cm];
            if f == i64::MIN {
                active[cm] = (t, t);
                touched.push(cm as u32);
            } else if t - l > WEEK {
                emit(cm, f, l, acc);
                active[cm] = (t, t);
            } else {
                active[cm].1 = t;
            }
        }
        for &cm in touched.iter() {
            let (f, l) = active[cm as usize];
            emit(cm as usize, f, l, acc);
            active[cm as usize] = (i64::MIN, i64::MIN);
        }
        touched.clear();
        si = sj;
        ci = cj;
    }
}

/// `ss --dump N DIR`: print the fields (hex; "-" for empty) of every well-formed N-field line of DIR/*.csv.
/// Used by tests/test_csv_grammar.py to compare this scanner with the reference parser.
fn dump(n: usize, dir: &Path) {
    let files = map_files(dir);
    let cs = chunks(&files);
    let hex = |f: &[u8]| if f.is_empty() { "-".to_string() } else { f.iter().map(|b| format!("{b:02x}")).collect::<String>() };
    macro_rules! go {
        ($n:literal) => {
            for c in &cs {
                scan::<$n>(&files[c.file][c.start..c.end], |_, f| {
                    println!("{}", f.iter().map(|x| hex(x)).collect::<Vec<_>>().join(" "));
                });
            }
        };
    }
    match n {
        2 => go!(2),
        4 => go!(4),
        7 => go!(7),
        _ => panic!("arity 2, 4 or 7"),
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() == 2 && args[1] == "--ts" {
        // test hook: one timestamp per stdin line (split on \n only), print epoch ms or "None"
        use std::io::Read;
        let mut buf = vec![];
        std::io::stdin().read_to_end(&mut buf).unwrap();
        let mut dc = DateCache::new();
        let mut lines: Vec<&[u8]> = buf.split(|&c| c == b'\n').collect();
        lines.pop();
        for l in lines {
            match parse_ts(l, &mut dc) {
                Some(v) => println!("{v}"),
                None => println!("None"),
            }
        }
        return;
    }
    if args.len() == 4 && args[1] == "--dump" {
        dump(args[2].parse().unwrap(), Path::new(&args[3]));
        return;
    }
    let lg = Logger(Instant::now());
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into()));
    let out = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into()));
    let ncpu: usize = std::env::var("NCPU").ok().and_then(|s| s.parse().ok()).filter(|&n| n > 0).unwrap_or(8);
    rayon::ThreadPoolBuilder::new().num_threads(ncpu).build_global().unwrap();

    let manifest = std::fs::read_to_string(data.join("manifest.json")).expect("manifest.json");
    let mut dc0 = DateCache::new();
    let ws = parse_ts(json_str(&manifest, "window_start").expect("window_start").as_bytes(), &mut dc0).expect("window_start");
    let we = parse_ts(json_str(&manifest, "window_end").expect("window_end").as_bytes(), &mut dc0).expect("window_end");
    let cws = ws - WEEK;
    if we <= ws || we - cws >= (1 << 31) || ws < 0 {
        decline("window too long or malformed for packed keys");
    }
    let ws_day = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - ws_day + 1) as usize;

    // ---- dimension tables (clean files)
    let read_pairs = |name: &str| -> Vec<(Vec<u8>, Vec<u8>)> {
        let txt = std::fs::read(data.join(name)).unwrap_or_default();
        let mut v = vec![];
        let mut first = true;
        for_lines(&txt, |l| {
            if first {
                first = false;
                return;
            }
            if let Some([a, b]) = split::<2>(l) {
                v.push((a.to_vec(), b.to_vec()));
            }
        });
        v
    };
    let products: Vec<(Vec<u8>, Vec<u8>)> = read_pairs("products.csv");
    let campaigns: Vec<(Vec<u8>, Vec<u8>)> = read_pairs("campaigns.csv");
    let mut cat_names: Vec<Vec<u8>> = products.iter().map(|p| p.1.clone()).collect();
    cat_names.push(b"unknown".to_vec());
    cat_names.sort();
    cat_names.dedup();
    if cat_names.len() > 256 {
        decline("too many categories");
    }
    let cat_id: FMap<&[u8], u16> = cat_names.iter().enumerate().map(|(i, c)| (c.as_slice(), i as u16)).collect();
    let unknown_cat = cat_id[b"unknown".as_slice()];
    let prod: FMap<&[u8], u16> = products.iter().map(|(p, c)| (p.as_slice(), cat_id[c.as_slice()])).collect();
    let camp_idx: FMap<&[u8], u32> =
        campaigns.iter().enumerate().map(|(i, (c, _))| (c.as_slice(), i as u32)).collect();
    let ncamp = campaigns.len();
    if (ncamp + 1) * ndays > 100_000_000 {
        decline("campaign x day table too large");
    }
    lg.log("dims");

    // ---- links
    let lk_files = map_files(&data.join("links"));
    let lk_chunks = chunks(&lk_files);
    let links: Vec<(u64, u32, Raw, u64, u32, Raw)> = lk_chunks
        .par_iter()
        .flat_map_iter(|c| {
            let b = &lk_files[c.file][c.start..c.end];
            let mut v = vec![];
            scan::<2>(b, |_, [a, bb]| {
                if !a.is_empty() && !bb.is_empty() {
                    let ((ha, ka), (hb, kb)) = (uhash(a), uhash(bb));
                    v.push((ha, ka, Raw::of(a), hb, kb, Raw::of(bb)));
                }
            });
            v
        })
        .collect();
    lg.log(&format!("links: {}", links.len()));
    // Link components are computed on a side thread while events and clicks parse.
    let links = Arc::new(links);
    let link_thread = {
        let links = Arc::clone(&links);
        std::thread::spawn(move || link_components(&links))
    };

    // ---- events: parse + hash-partition
    let ev_files = map_files(&data.join("events"));
    let ev_chunks = chunks(&ev_files);
    // Every parsed user id also goes straight into the universe shards (users of dropped lines only add people
    // without events, who never reach the output), so the universe does not have to wait for dedup.
    const NBU: usize = 256;
    type Part = Vec<Vec<(u64, u32, Raw)>>; // (hash, check, user id bytes)
    let shard = |uh: u64| ((uh >> 48) & 0xff) as usize;
    let (evparts, mut uparts): (Vec<Vec<Vec<Ev>>>, Vec<Part>) = ev_chunks
        .par_iter()
        .map(|c| {
            let b = &ev_files[c.file][c.start..c.end];
            let mut dc = DateCache::new();
            let est = b.len() / 80 / NB + 8;
            let mut buckets: Vec<Vec<Ev>> = (0..NB).map(|_| Vec::with_capacity(est)).collect();
            let uest = b.len() / 80 / NBU * 5 / 4 + 8;
            let mut ub: Part = (0..NBU).map(|_| Vec::with_capacity(uest)).collect();
            scan::<7>(b, |line, f| {
                if f[0].is_empty() {
                    return;
                }
                let h = hash_bytes(f[0]);
                let (uh, uk) = if f[1].is_empty() { (0, 0) } else { uhash(f[1]) };
                if uh != 0 {
                    ub[shard(uh)].push((uh, uk, Raw::of(f[1])));
                }
                let ing = match parse_ts(f[3], &mut dc) {
                    None => u32::MAX,
                    Some(ms) if ms >= 0 && ms % 1000 == 0 && ms / 1000 < u32::MAX as i64 => (ms / 1000) as u32,
                    Some(_) => decline("ingest_ts is not whole seconds in 1970..2106"),
                };
                let ts = parse_ts(f[2], &mut dc);
                let ty = f[4];
                let purchase = ty == b"purchase";
                let mut ok = !f[1].is_empty()
                    && !f[5].is_empty()
                    && (purchase || ty == b"view" || ty == b"cart")
                    && matches!(ts, Some(t) if t >= ws && t < we);
                let mut price = 0;
                if ok && purchase {
                    match parse_price(f[6]) {
                        Some(p) => price = p,
                        None => ok = false,
                    }
                }
                // the category is only used for purchase revenue; views and carts (97%) skip the lookup
                let cat = if ok && purchase { prod.get(f[5]).copied().unwrap_or(unknown_cat) } else { 0 };
                buckets[((h >> 24) as usize) & (NB - 1)].push(Ev {
                    h,
                    ing,
                    ts: if ok { (ts.unwrap() - ws) as u32 | 1 << 31 } else { 0 },
                    line: Raw::of(line),
                    uh,
                    pc: price | (cat as u32) << 24,
                });
            });
            (buckets, ub)
        })
        .unzip();
    lg.log("events parsed");

    // ---- clicks
    let ck_files = map_files(&data.join("clicks"));
    let ck_chunks = chunks(&ck_files);
    let (cparts, cuparts): (Vec<Vec<Vec<Ck>>>, Vec<Part>) = ck_chunks
        .par_iter()
        .map(|c| {
            let b = &ck_files[c.file][c.start..c.end];
            let mut dc = DateCache::new();
            let est = b.len() / 50 / NB + 8;
            let mut buckets: Vec<Vec<Ck>> = (0..NB).map(|_| Vec::with_capacity(est)).collect();
            let uest = b.len() / 50 / NBU * 5 / 4 + 8;
            let mut ub: Part = (0..NBU).map(|_| Vec::with_capacity(uest)).collect();
            scan::<4>(b, |line, f| {
                if f[0].is_empty() {
                    return;
                }
                let h = hash_bytes(f[0]);
                let (uh, uk) = if f[1].is_empty() { (0, 0) } else { uhash(f[1]) };
                if uh != 0 {
                    ub[shard(uh)].push((uh, uk, Raw::of(f[1])));
                }
                let ts = parse_ts(f[3], &mut dc);
                let camp = camp_idx.get(f[2]).copied();
                let ok = !f[1].is_empty() && camp.is_some() && matches!(ts, Some(t) if t >= cws && t < we);
                buckets[((h >> 24) as usize) & (NB - 1)].push(Ck {
                    h,
                    ts: if ok { (ts.unwrap() - cws) as u32 } else { 0 },
                    line: Raw::of(line),
                    uh,
                    uk,
                    camp: if ok { camp.unwrap() } else { u32::MAX },
                });
            });
            (buckets, ub)
        })
        .unzip();
    uparts.extend(cuparts);
    lg.log("clicks parsed");

    // ---- ID universe, byte-sorted: index order is byte order, so the smallest index in a component is its name.
    // IDs were hashed once at parse time and split into 256 shards; each shard's distinct set is small enough to
    // stay in cache and is built in parallel, so there is nothing to merge.
    {
        let mut lb: Part = (0..NBU).map(|_| Vec::new()).collect();
        for &(ha, ka, a, hb, kb, b) in links.iter() {
            lb[shard(ha)].push((ha, ka, a));
            lb[shard(hb)].push((hb, kb, b));
        }
        uparts.push(lb);
    }
    let parts = uparts;
    lg.log("  universe: sharded");
    let locals: Vec<(Vec<Raw>, IdMap<(u32, u32)>)> = (0..NBU)
        .into_par_iter()
        .map(|u| {
            let mut map: IdMap<(u32, u32)> = IdMap::default();
            let mut list: Vec<Raw> = vec![];
            for p in &parts {
                for &(uh, uk, r) in &p[u] {
                    match map.entry(uh) {
                        std::collections::hash_map::Entry::Occupied(o) => {
                            if o.get().0 != uk {
                                decline("64-bit hash collision between different ids");
                            }
                        }
                        std::collections::hash_map::Entry::Vacant(v) => {
                            v.insert((uk, list.len() as u32));
                            list.push(r);
                        }
                    }
                }
            }
            (list, map)
        })
        .collect();
    drop(parts);
    lg.log("  universe: shard tables");
    let mut base = vec![0u32; NBU + 1];
    for u in 0..NBU {
        base[u + 1] = base[u] + locals[u].0.len() as u32;
    }
    let n = base[NBU] as usize;
    if n >= u32::MAX as usize - 1 {
        decline("too many ids");
    }
    // A person is numbered by the dense universe index of its component's smallest id (bytes); that id's bytes
    // are the person's name. No global sort is needed: link components already know their smallest member.
    let uni: Vec<&'static [u8]> = locals.iter().flat_map(|(l, _)| l.iter().map(|r| r.get())).collect();
    lg.log(&format!("universe: {n} ids"));
    let lm = link_thread.join().expect("link thread");
    let pkt = PkTable::new(n);
    locals.par_iter().enumerate().for_each(|(u, (_, map))| {
        for (&uh, v) in map.iter() {
            let p = match lm.get(&uh) {
                None => base[u] + v.1,
                Some(&r) => {
                    let ru = shard(r);
                    base[ru] + locals[ru].1[&r].1
                }
            };
            pkt.insert(uh, p);
        }
    });
    lg.log("components");

    // ---- bucket events and clicks by person range, then do each bucket's work independently in parallel
    const NBK: usize = 1024;
    let nn = (n as u64).max(1);
    let bucket_of = |p: u32| ((p as u64 * NBK as u64) / nn) as usize;
    let evp: Vec<Vec<Vec<E>>> = (0..NB)
        .into_par_iter()
        .map(|b| {
            // dedup (earliest ingest_ts per event_id), keep valid and not late, and write the person record directly
            let mut v = dedup_bucket(
                &evparts,
                b,
                |e: &Ev| e.h,
                |e: &Ev| id_of(e.line),
                |a: &Ev, b: &Ev| a.ing < b.ing,
                |e: &Ev| (e.h, e.ing as u64),
            );
            v.retain(|e| e.ts >> 31 == 1 && e.ing as i64 * 1000 - (ws + (e.ts & 0x7fff_ffff) as i64) <= DAY);
            let est = v.len() / NBK + v.len() / NBK / 4 + 8;
            let mut lb: Vec<Vec<E>> = (0..NBK).map(|_| Vec::with_capacity(est)).collect();
            for chunk in v.chunks(16) {
                for e in chunk {
                    pkt.prefetch(e.uh);
                }
                for e in chunk {
                    let p = pkt.get(e.uh);
                    lb[bucket_of(p)].push(E {
                        key: ((p as u64) << 32) | (e.ts & 0x7fff_ffff) as u64,
                        price: e.pc & 0xff_ffff,
                        cat: (e.pc >> 24) as u16,
                        purchase: e.pc != 0,
                    });
                }
            }
            lb
        })
        .collect();
    drop(evparts);
    let ckp: Vec<Vec<Vec<C>>> = (0..NB)
        .into_par_iter()
        .map(|b| {
            let mut v = dedup_bucket(
                &cparts,
                b,
                |c: &Ck| c.h,
                |c: &Ck| id_of(c.line),
                |_: &Ck, _: &Ck| false, // copies of a click are identical: keep the first
                |c: &Ck| (c.h, 0),
            );
            v.retain(|c| c.camp != u32::MAX);
            let est = v.len() / NBK + v.len() / NBK / 4 + 8;
            let mut lb: Vec<Vec<C>> = (0..NBK).map(|_| Vec::with_capacity(est)).collect();
            for chunk in v.chunks(16) {
                for c in chunk {
                    pkt.prefetch(c.uh);
                }
                for c in chunk {
                    let p = pkt.get(c.uh);
                    lb[bucket_of(p)].push(C { key: ((p as u64) << 32) | c.ts as u64, camp: c.camp, id: c.line });
                }
            }
            lb
        })
        .collect();
    drop(cparts);
    lg.log("events and clicks deduped and bucketed by person");

    let ncat = cat_names.len();
    let cells = (ncamp + 1) * ndays;
    let geo = Geometry { ws, ws_day, cws, ncamp, ndays };
    let Worker { acc, urows, .. } = (0..NBK)
        .into_par_iter()
        .fold(
            || Worker::new(cells, ncamp, ncat),
            |mut w, b| {
                run_bucket(&mut w, b, &evp, &ckp, &geo);
                w
            },
        )
        .reduce(|| Worker::new(cells, ncamp, ncat), |a, b| a.merge(b));
    drop(evp);
    drop(ckp);
    lg.log(&format!("sessions, credit, assists: user_daily rows {}", urows.len()));

    // ---- output
    std::fs::create_dir_all(out.join("user_daily")).expect("output dir");
    std::fs::create_dir_all(out.join("campaign_daily")).expect("output dir");
    let nparts = ncpu.max(1);

    let u_schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("events", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("top_category", DataType::Utf8, true),
    ]));
    let per = urows.len().div_ceil(nparts).max(1);
    (0..nparts).into_par_iter().for_each(|i| {
        let rows = &urows[(i * per).min(urows.len())..((i + 1) * per).min(urows.len())];
        if rows.is_empty() && i > 0 {
            return;
        }
        let mut uid = StringBuilder::new();
        let mut day = Date32Builder::new();
        let (mut ss, mut ee, mut rr) = (Int64Builder::new(), Int64Builder::new(), Int64Builder::new());
        let mut top = StringBuilder::new();
        for r in rows {
            uid.append_value(String::from_utf8_lossy(uni[r.pk as usize]));
            day.append_value((ws_day + r.day as i64) as i32);
            ss.append_value(r.sessions as i64);
            ee.append_value(r.events as i64);
            rr.append_value(r.rev as i64);
            if r.top == u16::MAX {
                top.append_null();
            } else {
                top.append_value(String::from_utf8_lossy(&cat_names[r.top as usize]));
            }
        }
        let cols: Vec<ArrayRef> = vec![
            Arc::new(uid.finish()),
            Arc::new(day.finish()),
            Arc::new(ss.finish()),
            Arc::new(ee.finish()),
            Arc::new(rr.finish()),
            Arc::new(top.finish()),
        ];
        write_part(&out.join("user_daily").join(format!("part-{i}.parquet")), &u_schema, cols);
    });

    let c_schema = Arc::new(Schema::new(vec![
        Field::new("campaign_id", DataType::Utf8, false),
        Field::new("channel", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("purchasing_sessions", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("assisted_sessions", DataType::Int64, false),
    ]));
    let (mut cid, mut ch) = (StringBuilder::new(), StringBuilder::new());
    let mut day = Date32Builder::new();
    let (mut s1, mut s2, mut s3, mut s4) =
        (Int64Builder::new(), Int64Builder::new(), Int64Builder::new(), Int64Builder::new());
    for camp in 0..=ncamp {
        for d in 0..ndays {
            let cell = camp * ndays + d;
            let asst = if camp < ncamp { acc.asst[cell] } else { 0 };
            if acc.sess[cell] == 0 && asst == 0 {
                continue;
            }
            if camp < ncamp {
                cid.append_value(String::from_utf8_lossy(&campaigns[camp].0));
                ch.append_value(String::from_utf8_lossy(&campaigns[camp].1));
            } else {
                cid.append_value("organic");
                ch.append_value("organic");
            }
            day.append_value((ws_day + d as i64) as i32);
            s1.append_value(acc.sess[cell] as i64);
            s2.append_value(acc.purch[cell] as i64);
            s3.append_value(acc.rev[cell] as i64);
            s4.append_value(asst as i64);
        }
    }
    let cols: Vec<ArrayRef> = vec![
        Arc::new(cid.finish()),
        Arc::new(ch.finish()),
        Arc::new(day.finish()),
        Arc::new(s1.finish()),
        Arc::new(s2.finish()),
        Arc::new(s3.finish()),
        Arc::new(s4.finish()),
    ];
    write_part(&out.join("campaign_daily").join("part-0.parquet"), &c_schema, cols);
    lg.log("done");
}

fn write_part(path: &Path, schema: &Arc<Schema>, cols: Vec<ArrayRef>) {
    let props = WriterProperties::builder().set_compression(Compression::UNCOMPRESSED).set_dictionary_enabled(false).build();
    let f = std::fs::File::create(path).expect("create parquet");
    let mut w = ArrowWriter::try_new(f, schema.clone(), Some(props)).expect("parquet writer");
    let batch = RecordBatch::try_new(schema.clone(), cols).expect("batch");
    if batch.num_rows() > 0 {
        w.write(&batch).expect("write");
    }
    w.close().expect("close");
}
