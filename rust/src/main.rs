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
impl Logger {
    fn log(&self, what: &str) {
        eprintln!("[{:7.3}s] {}", self.0.elapsed().as_secs_f64(), what);
    }
}

fn decline(why: &str) -> ! {
    eprintln!("ss: declining: {why}");
    std::process::exit(DECLINE);
}

// ---------------------------------------------------------------- bytes helpers

/// A byte slice stored as raw parts. Every slice points into an Mmap that lives until process exit.
#[derive(Clone, Copy)]
struct Raw {
    p: usize,
    l: u32,
}
impl Raw {
    #[inline]
    fn of(s: &[u8]) -> Raw {
        Raw { p: s.as_ptr() as usize, l: s.len() as u32 }
    }
    #[inline]
    fn get<'a>(self) -> &'a [u8] {
        // SAFETY: created from a slice of an Mmap (or a leaked buffer) that is never dropped.
        unsafe { std::slice::from_raw_parts(self.p as *const u8, self.l as usize) }
    }
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

/// Split a line into exactly N well-formed fields.
#[inline]
fn split<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    let mut out: [&[u8]; N] = [&line[0..0]; N];
    let mut s = 0;
    let mut i = 0;
    for p in memchr_iter(b',', line) {
        if i == N - 1 {
            return None;
        }
        out[i] = field(&line[s..p])?;
        i += 1;
        s = p + 1;
    }
    if i != N - 1 {
        return None;
    }
    out[i] = field(&line[s..])?;
    Some(out)
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
    let (a, b) = (s[i], s[i + 1]);
    if a.is_ascii_digit() && b.is_ascii_digit() {
        Some(((a - b'0') * 10 + (b - b'0')) as i64)
    } else {
        None
    }
}

/// rules.md "Timestamps": F1, F2, F3 to epoch ms UTC, or None. Digits are ASCII only.
fn parse_ts(s: &[u8]) -> Option<i64> {
    let n = s.len();
    if n == 0 {
        return None;
    }
    if s.iter().all(|c| c.is_ascii_digit()) {
        let mut i = 0;
        while i < n && s[i] == b'0' {
            i += 1;
        }
        if n - i > 15 {
            return None;
        }
        let mut v: i64 = 0;
        for &c in &s[i..] {
            v = v * 10 + (c - b'0') as i64;
        }
        return Some(v);
    }
    if n != 20 && n != 25 {
        return None;
    }
    if s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let y = d2(s, 0)? * 100 + d2(s, 2)?;
    let (mo, d, h, mi, se) = (d2(s, 5)?, d2(s, 8)?, d2(s, 11)?, d2(s, 14)?, d2(s, 17)?);
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
    if !(1..=12).contains(&mo) || h > 23 || mi > 59 || se > 59 || d < 1 {
        return None;
    }
    let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
    let dim = match mo {
        2 => {
            if leap {
                29
            } else {
                28
            }
        }
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    if d > dim {
        return None;
    }
    Some((((days_from_civil(y, mo, d) * 24 + h) * 60 + mi) * 60 + se) * 1000 - off)
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

#[derive(Clone, Copy)]
struct Ev {
    h: u64,
    ing: i64,
    ts: i64,
    id: Raw,
    user: Raw,
    price: u32,
    cat: u16,
    ok: bool,
    purchase: bool,
}

#[derive(Clone, Copy)]
struct Ck {
    h: u64,
    ts: i64,
    id: Raw,
    user: Raw,
    camp: u32,
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
    fn add(mut self, o: Acc) -> Acc {
        for i in 0..self.sess.len() {
            self.sess[i] += o.sess[i];
            self.purch[i] += o.purch[i];
            self.rev[i] += o.rev[i];
            self.asst[i] += o.asst[i];
        }
        self
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

fn main() {
    let lg = Logger(Instant::now());
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into()));
    let out = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into()));
    let ncpu: usize = std::env::var("NCPU").ok().and_then(|s| s.parse().ok()).filter(|&n| n > 0).unwrap_or(8);
    rayon::ThreadPoolBuilder::new().num_threads(ncpu).build_global().unwrap();

    let manifest = std::fs::read_to_string(data.join("manifest.json")).expect("manifest.json");
    let ws = parse_ts(json_str(&manifest, "window_start").expect("window_start").as_bytes()).expect("window_start");
    let we = parse_ts(json_str(&manifest, "window_end").expect("window_end").as_bytes()).expect("window_end");
    let cws = ws - WEEK;
    if we <= ws || we - cws >= (1 << 32) || ws < 0 {
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
    if cat_names.len() >= u16::MAX as usize - 1 {
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

    // ---- events: parse + hash-partition
    let ev_files = map_files(&data.join("events"));
    let ev_chunks = chunks(&ev_files);
    let parts: Vec<Vec<Vec<Ev>>> = ev_chunks
        .par_iter()
        .map(|c| {
            let b = &ev_files[c.file][c.start..c.end];
            let mut buckets: Vec<Vec<Ev>> = (0..NB).map(|_| Vec::new()).collect();
            for_lines(b, |line| {
                let Some(f) = split::<7>(line) else { return };
                if f[0].is_empty() {
                    return;
                }
                let h = hash_bytes(f[0]);
                let ing = parse_ts(f[3]).unwrap_or(i64::MAX);
                let ts = parse_ts(f[2]);
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
                let cat = if ok { prod.get(f[5]).copied().unwrap_or(unknown_cat) } else { 0 };
                buckets[(h >> 56) as usize].push(Ev {
                    h,
                    ing,
                    ts: ts.unwrap_or(0),
                    id: Raw::of(f[0]),
                    user: Raw::of(f[1]),
                    price,
                    cat,
                    ok,
                    purchase,
                });
            });
            buckets
        })
        .collect();
    lg.log("events parsed");

    // ---- events: dedup (earliest ingest_ts per event_id), then keep valid and not late
    let ev_surv: Vec<Vec<Ev>> = (0..NB)
        .into_par_iter()
        .map(|b| {
            let total: usize = parts.iter().map(|p| p[b].len()).sum();
            let mut v: Vec<Ev> = Vec::with_capacity(total);
            for p in &parts {
                v.extend_from_slice(&p[b]);
            }
            v.sort_unstable_by_key(|e| (e.h, e.ing));
            let mut keep = Vec::with_capacity(v.len());
            dedup_sorted(&v, |e| e.h, |e| e.id.get(), |e| {
                if e.ok && e.ing - e.ts <= DAY {
                    keep.push(e)
                }
            });
            keep
        })
        .collect();
    drop(parts);
    lg.log(&format!("events deduped: {} kept", ev_surv.iter().map(|v| v.len()).sum::<usize>()));

    // ---- clicks
    let ck_files = map_files(&data.join("clicks"));
    let ck_chunks = chunks(&ck_files);
    let cparts: Vec<Vec<Vec<Ck>>> = ck_chunks
        .par_iter()
        .map(|c| {
            let b = &ck_files[c.file][c.start..c.end];
            let mut buckets: Vec<Vec<Ck>> = (0..NB).map(|_| Vec::new()).collect();
            for_lines(b, |line| {
                let Some(f) = split::<4>(line) else { return };
                if f[0].is_empty() {
                    return;
                }
                let h = hash_bytes(f[0]);
                let ts = parse_ts(f[3]);
                let camp = camp_idx.get(f[2]).copied();
                let ok = !f[1].is_empty() && camp.is_some() && matches!(ts, Some(t) if t >= cws && t < we);
                buckets[(h >> 56) as usize].push(Ck {
                    h,
                    ts: ts.unwrap_or(0),
                    id: Raw::of(f[0]),
                    user: Raw::of(f[1]),
                    camp: camp.unwrap_or(0),
                    // validity packed into camp via the sentinel below
                });
                if !ok {
                    buckets[(h >> 56) as usize].last_mut().unwrap().camp = u32::MAX;
                }
            });
            buckets
        })
        .collect();
    let ck_surv: Vec<Vec<Ck>> = (0..NB)
        .into_par_iter()
        .map(|b| {
            let total: usize = cparts.iter().map(|p| p[b].len()).sum();
            let mut v: Vec<Ck> = Vec::with_capacity(total);
            for p in &cparts {
                v.extend_from_slice(&p[b]);
            }
            v.sort_unstable_by_key(|c| c.h);
            let mut keep = Vec::new();
            dedup_sorted(&v, |c| c.h, |c| c.id.get(), |c| {
                if c.camp != u32::MAX {
                    keep.push(c)
                }
            });
            keep
        })
        .collect();
    drop(cparts);
    lg.log(&format!("clicks parsed+deduped: {} kept", ck_surv.iter().map(|v| v.len()).sum::<usize>()));

    // ---- links
    let lk_files = map_files(&data.join("links"));
    let lk_chunks = chunks(&lk_files);
    let links: Vec<(Raw, Raw)> = lk_chunks
        .par_iter()
        .flat_map_iter(|c| {
            let b = &lk_files[c.file][c.start..c.end];
            let mut v = vec![];
            for_lines(b, |line| {
                if let Some([a, bb]) = split::<2>(line) {
                    if !a.is_empty() && !bb.is_empty() {
                        v.push((Raw::of(a), Raw::of(bb)));
                    }
                }
            });
            v
        })
        .collect();
    lg.log(&format!("links: {}", links.len()));

    // ---- ID universe, byte-sorted: index order is byte order, so the smallest index in a component is its name
    let merge = |mut a: FSet<&'static [u8]>, b: FSet<&'static [u8]>| {
        if a.len() < b.len() {
            let mut b = b;
            std::mem::swap(&mut a, &mut b);
            a.extend(b);
            a
        } else {
            a.extend(b);
            a
        }
    };
    let ev_set = ev_surv
        .par_iter()
        .fold(FSet::<&'static [u8]>::default, |mut s, v| {
            for e in v {
                s.insert(e.user.get());
            }
            s
        })
        .reduce(FSet::default, merge);
    let ck_set = ck_surv
        .par_iter()
        .fold(FSet::<&'static [u8]>::default, |mut s, v| {
            for c in v {
                s.insert(c.user.get());
            }
            s
        })
        .reduce(FSet::default, merge);
    let mut all = merge(ev_set, ck_set);
    for (a, b) in &links {
        all.insert(a.get());
        all.insert(b.get());
    }
    let mut uni: Vec<&'static [u8]> = all.into_iter().collect();
    uni.par_sort_unstable();
    let n = uni.len();
    if n >= u32::MAX as usize - 1 {
        decline("too many ids");
    }
    let mut id_idx: FMap<&[u8], u32> = FMap::with_capacity_and_hasher(n, FixedState::default());
    for (i, s) in uni.iter().enumerate() {
        id_idx.insert(s, i as u32);
    }
    lg.log(&format!("universe: {n} ids"));

    // ---- components, named by the smallest index
    let mut parent: Vec<u32> = (0..n as u32).collect();
    for (a, b) in &links {
        let (ia, ib) = (id_idx[a.get()], id_idx[b.get()]);
        let (ra, rb) = (find(&mut parent, ia), find(&mut parent, ib));
        if ra != rb {
            let (lo, hi) = if ra < rb { (ra, rb) } else { (rb, ra) };
            parent[hi as usize] = lo;
        }
    }
    let pk: Vec<u32> = (0..n as u32).map(|i| find(&mut parent, i)).collect();
    lg.log("components");

    // ---- packed events, one parallel sort
    let mut evs: Vec<E> = ev_surv
        .par_iter()
        .flat_map_iter(|v| {
            v.iter().map(|e| E {
                key: ((pk[id_idx[e.user.get()] as usize] as u64) << 32) | (e.ts - ws) as u64,
                price: e.price,
                cat: e.cat,
                purchase: e.purchase,
            })
        })
        .collect();
    drop(ev_surv);
    evs.par_sort_unstable_by_key(|e| e.key);
    lg.log(&format!("events sorted: {}", evs.len()));

    // ---- sessions and user_daily in one sequential pass
    let ncat = cat_names.len();
    let mut sess: Vec<Sess> = Vec::new();
    let mut urows: Vec<UserRow> = Vec::new();
    {
        let mut catrev = vec![0u64; ncat];
        let mut touched: Vec<u16> = vec![];
        let mut cur: Option<(u32, u16)> = None; // (person, day) group
        let (mut g_sessions, mut g_events, mut g_rev) = (0u32, 0u64, 0u64);
        let mut prev_key = 0u64;
        let mut open = false;
        macro_rules! flush {
            () => {
                if let Some((p, d)) = cur {
                    let mut top = u16::MAX;
                    let mut best = 0u64;
                    for &c in &touched {
                        let r = catrev[c as usize];
                        if r > best || (r == best && r > 0 && c < top) {
                            best = r;
                            top = c;
                        }
                        catrev[c as usize] = 0;
                    }
                    touched.clear();
                    urows.push(UserRow { pk: p, day: d, sessions: g_sessions, events: g_events, rev: g_rev, top });
                    g_sessions = 0;
                    g_events = 0;
                    g_rev = 0;
                }
            };
        }
        for e in &evs {
            let p = (e.key >> 32) as u32;
            let new = !open || (e.key >> 32) != (prev_key >> 32) || (e.key & 0xffff_ffff) - (prev_key & 0xffff_ffff) > GAP as u64;
            if new {
                let start = ws + (e.key & 0xffff_ffff) as i64;
                let day = (start.div_euclid(DAY) - ws_day) as u16;
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
                    touched.push(e.cat);
                }
                catrev[e.cat as usize] += e.price as u64;
            }
        }
        flush!();
    }
    drop(evs);
    lg.log(&format!("sessions: {}, user_daily rows: {}", sess.len(), urows.len()));

    // ---- clicks packed + sorted by (person, ts, click_id bytes)
    let mut cks: Vec<C> = ck_surv
        .par_iter()
        .flat_map_iter(|v| {
            v.iter().map(|c| C {
                key: ((pk[id_idx[c.user.get()] as usize] as u64) << 32) | (c.ts - cws) as u64,
                camp: c.camp,
                id: c.id,
            })
        })
        .collect();
    drop(ck_surv);
    cks.par_sort_unstable_by(|a, b| a.key.cmp(&b.key).then_with(|| a.id.get().cmp(b.id.get())));
    lg.log("clicks sorted");

    // ---- credit + assists per person
    let mut tasks: Vec<(usize, usize, usize, usize)> = vec![]; // (sess lo, hi, click lo, hi)
    {
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
            tasks.push((si, sj, ci, cj));
            si = sj;
            ci = cj;
        }
    }
    let cells = (ncamp + 1) * ndays;
    let acc = tasks
        .par_chunks(256)
        .fold(
            || (Acc::new(cells), vec![(i64::MIN, i64::MIN); ncamp], Vec::<u32>::new()),
            |(mut acc, mut active, mut touched), chunk| {
                for &(slo, shi, clo, chi) in chunk {
                    let ss = &sess[slo..shi];
                    let cs = &cks[clo..chi];
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
                    let mut emit = |camp: usize, first: i64, last: i64, acc: &mut Acc| {
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
                            emit(cm, f, l, &mut acc);
                            active[cm] = (t, t);
                        } else {
                            active[cm].1 = t;
                        }
                    }
                    for &cm in &touched {
                        let (f, l) = active[cm as usize];
                        emit(cm as usize, f, l, &mut acc);
                        active[cm as usize] = (i64::MIN, i64::MIN);
                    }
                    touched.clear();
                }
                (acc, active, touched)
            },
        )
        .map(|(a, _, _)| a)
        .reduce(|| Acc::new(cells), |a, b| a.add(b));
    lg.log("credit + assists");

    // ---- output
    std::fs::create_dir_all(out.join("user_daily")).expect("output dir");
    std::fs::create_dir_all(out.join("campaign_daily")).expect("output dir");
    let nparts = ncpu.max(1);
    let name_of = |p: u32| String::from_utf8_lossy(uni[p as usize]).into_owned();

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
            uid.append_value(name_of(r.pk));
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
    let props = WriterProperties::builder().set_compression(Compression::UNCOMPRESSED).build();
    let f = std::fs::File::create(path).expect("create parquet");
    let mut w = ArrowWriter::try_new(f, schema.clone(), Some(props)).expect("parquet writer");
    let batch = RecordBatch::try_new(schema.clone(), cols).expect("batch");
    if batch.num_rows() > 0 {
        w.write(&batch).expect("write");
    }
    w.close().expect("close");
}
