//! Durable history of Location Pings, for replay and Reconciliation. Matching never waits
//! on the disk: a background thread batches records, and another compresses closed segments.
//!
//! Each record is `length, checksum, payload`, so a crash that tears the end of a segment
//! costs only the torn record. Reading stops at the first record that doesn't check out.

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, ErrorKind, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, SyncSender, TrySendError, channel, sync_channel};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use geo::Position;

const OPEN: &str = "log";
const CLOSED: &str = "log.zst";
const COMPRESSING: &str = "log.zst.tmp";
const COMPRESSION_LEVEL: i32 = 3;
const HEADER_BYTES: usize = 8;
const FIXED_PAYLOAD_BYTES: usize = 32;
const MAX_PAYLOAD_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone)]
pub struct Config {
    pub dir: PathBuf,
    /// Pings older than this are deleted once their whole segment has aged out.
    pub retention: Duration,
    /// Oldest closed segments are deleted past this size, even within `retention`.
    pub max_bytes: u64,
    pub sync_every: Duration,
    pub segment_bytes: u64,
    pub segment_age: Duration,
    /// Pings waiting for the writer. When full, pings are counted as dropped, not waited on.
    pub queue_capacity: usize,
}

impl Config {
    pub fn new(dir: PathBuf) -> Self {
        Self {
            dir,
            retention: Duration::from_secs(24 * 3600),
            max_bytes: 20_000_000_000,
            sync_every: Duration::from_secs(1),
            segment_bytes: 64 * 1024 * 1024,
            segment_age: Duration::from_secs(300),
            queue_capacity: 200_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PingRecord {
    pub driver: String,
    pub reported_at_ms: i64,
    pub received_at_ms: i64,
    pub position: Position,
}

pub struct PingLog {
    queue: Option<SyncSender<Vec<u8>>>,
    writer: Option<JoinHandle<()>>,
    compressor: Option<JoinHandle<()>>,
    dropped: Arc<AtomicU64>,
}

impl PingLog {
    /// Opens the directory, and finishes what any earlier run left: leftover open segments
    /// are compressed in the background, keeping the valid records they hold.
    pub fn open(config: Config) -> io::Result<Self> {
        fs::create_dir_all(&config.dir)?;
        let mut leftovers = Vec::new();
        for path in segment_paths(&config.dir)? {
            if has_kind(&path, COMPRESSING) {
                fs::remove_file(&path)?;
            } else if has_kind(&path, OPEN) {
                if path.with_extension("log.zst").exists() {
                    // Compressed before a crash, but not yet removed.
                    fs::remove_file(&path)?;
                } else {
                    leftovers.push(path);
                }
            }
        }

        let (closed, to_compress) = channel();
        for path in leftovers {
            let _ = closed.send(path);
        }
        let (queue, records) = sync_channel(config.queue_capacity);
        let dropped = Arc::new(AtomicU64::new(0));
        let segment = Segment::create(&config.dir)?;

        let compressor = {
            let config = config.clone();
            std::thread::spawn(move || run_compressor(&config, &to_compress))
        };
        let writer = {
            let dropped = Arc::clone(&dropped);
            std::thread::spawn(move || run_writer(&config, segment, &records, &closed, &dropped))
        };
        Ok(Self { queue: Some(queue), writer: Some(writer), compressor: Some(compressor), dropped })
    }

    /// Queues a record without waiting. Returns false if it had to be dropped.
    pub fn append(&self, record: &PingRecord) -> bool {
        let Some(queue) = &self.queue else { return false };
        match queue.try_send(encode(record)) {
            Ok(()) => true,
            Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                false
            }
        }
    }

    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

impl Drop for PingLog {
    /// A clean shutdown: everything queued reaches the disk and every segment is compressed.
    fn drop(&mut self) {
        self.queue.take();
        for thread in [self.writer.take(), self.compressor.take()].into_iter().flatten() {
            let _ = thread.join();
        }
    }
}

struct Segment {
    path: PathBuf,
    file: BufWriter<File>,
    bytes: u64,
    opened: Instant,
}

impl Segment {
    fn create(dir: &Path) -> io::Result<Self> {
        let start_ms = SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |elapsed| elapsed.as_millis());
        for sequence in 0_u32.. {
            let path = dir.join(format!("{start_ms:020}-{sequence:04}.{OPEN}"));
            match OpenOptions::new().append(true).create_new(true).open(&path) {
                Ok(file) => {
                    return Ok(Self { path, file: BufWriter::new(file), bytes: 0, opened: Instant::now() });
                }
                Err(error) if error.kind() == ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error),
            }
        }
        unreachable!("the sequence always finds a free name")
    }

    fn sync(&mut self) -> io::Result<()> {
        self.file.flush()?;
        self.file.get_ref().sync_data()
    }
}

fn run_writer(
    config: &Config,
    mut segment: Segment,
    records: &Receiver<Vec<u8>>,
    closed: &Sender<PathBuf>,
    dropped: &AtomicU64,
) {
    let mut next_sync = Instant::now() + config.sync_every;
    let mut reported_dropped = 0;
    loop {
        let received = records.recv_timeout(next_sync.saturating_duration_since(Instant::now()));
        match received {
            Ok(bytes) => {
                if let Err(error) = segment.file.write_all(&bytes) {
                    eprintln!("ping log write failed: {error}");
                }
                segment.bytes += bytes.len() as u64;
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => break,
        }
        let now = Instant::now();
        if now < next_sync && segment.bytes < config.segment_bytes {
            continue;
        }
        if let Err(error) = segment.sync() {
            eprintln!("ping log sync failed: {error}");
        }
        next_sync = now + config.sync_every;
        let total_dropped = dropped.load(Ordering::Relaxed);
        if total_dropped != reported_dropped {
            eprintln!("ping log dropped {total_dropped} pings so far");
            reported_dropped = total_dropped;
        }
        if segment.bytes >= config.segment_bytes || segment.opened.elapsed() >= config.segment_age {
            match Segment::create(&config.dir) {
                Ok(next) => {
                    let _ = closed.send(std::mem::replace(&mut segment, next).path);
                }
                Err(error) => eprintln!("ping log rotation failed: {error}"),
            }
        }
    }
    if let Err(error) = segment.sync() {
        eprintln!("ping log sync failed: {error}");
    }
    let _ = closed.send(segment.path);
}

fn run_compressor(config: &Config, to_compress: &Receiver<PathBuf>) {
    for path in to_compress {
        if let Err(error) = compress(&path) {
            eprintln!("ping log compression failed for {}: {error}", path.display());
        }
        if let Err(error) = enforce_retention(&config.dir, config.retention, config.max_bytes, unix_ms_now()) {
            eprintln!("ping log retention failed: {error}");
        }
    }
}

fn unix_ms_now() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |elapsed| elapsed.as_millis())
}

/// Writes the valid records to a temporary name and renames it, so a crash at any point
/// leaves either the open segment or the finished compressed one.
fn compress(path: &Path) -> io::Result<()> {
    let temporary = path.with_extension(COMPRESSING);
    let mut encoder = zstd::Encoder::new(File::create(&temporary)?, COMPRESSION_LEVEL)?;
    let mut reader = RecordReader(BufReader::new(File::open(path)?));
    while let Some(bytes) = reader.next_raw() {
        encoder.write_all(&bytes)?;
    }
    encoder.finish()?.sync_all()?;
    fs::rename(&temporary, path.with_extension(CLOSED))?;
    fs::remove_file(path)
}

/// Deletes closed segments that have aged out, then the oldest until the total fits. Open
/// segments count towards the total, but only closed ones can be deleted.
fn enforce_retention(dir: &Path, retention: Duration, max_bytes: u64, now_ms: u128) -> io::Result<()> {
    let paths: Vec<PathBuf> =
        segment_paths(dir)?.into_iter().filter(|path| !has_kind(path, COMPRESSING)).collect();
    let cutoff = now_ms.saturating_sub(retention.as_millis());
    let mut total = 0;
    for path in &paths {
        total += fs::metadata(path)?.len();
    }
    for (index, path) in paths.iter().enumerate() {
        if !has_kind(path, CLOSED) {
            continue;
        }
        // A segment's pings all predate the start of the next one.
        let aged_out = paths.get(index + 1).and_then(|next| start_ms(next)).is_some_and(|next| next <= cutoff);
        if !aged_out && total <= max_bytes {
            continue;
        }
        total -= fs::metadata(path)?.len();
        fs::remove_file(path)?;
    }
    Ok(())
}

fn start_ms(path: &Path) -> Option<u128> {
    path.file_name()?.to_str()?.split('-').next()?.parse().ok()
}

fn has_kind(path: &Path, kind: &str) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.split_once('.'))
        .is_some_and(|(_, extension)| extension == kind)
}

/// Segment files in time order.
fn segment_paths(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if [OPEN, CLOSED, COMPRESSING].iter().any(|kind| has_kind(&path, kind)) {
            paths.push(path);
        }
    }
    paths.sort();
    Ok(paths)
}

/// Every readable record in the directory, oldest first.
pub fn read_all(dir: &Path) -> io::Result<Vec<PingRecord>> {
    let mut records = Vec::new();
    for path in segment_paths(dir)? {
        if has_kind(&path, COMPRESSING) {
            continue;
        }
        let file = BufReader::new(File::open(&path)?);
        if has_kind(&path, CLOSED) {
            collect(zstd::Decoder::with_buffer(file)?, &mut records);
        } else {
            collect(file, &mut records);
        }
    }
    Ok(records)
}

fn collect(source: impl Read, records: &mut Vec<PingRecord>) {
    let mut reader = RecordReader(source);
    while let Some(bytes) = reader.next_raw() {
        if let Some(record) = decode(&bytes[HEADER_BYTES..]) {
            records.push(record);
        }
    }
}

struct RecordReader<R>(R);

impl<R: Read> RecordReader<R> {
    /// The next whole record with its header, or None at the end or at a torn record.
    fn next_raw(&mut self) -> Option<Vec<u8>> {
        let mut header = [0; HEADER_BYTES];
        self.0.read_exact(&mut header).ok()?;
        let length = u32::from_le_bytes(header[..4].try_into().ok()?) as usize;
        if !(FIXED_PAYLOAD_BYTES..=MAX_PAYLOAD_BYTES).contains(&length) {
            return None;
        }
        let mut bytes = vec![0; HEADER_BYTES + length];
        bytes[..HEADER_BYTES].copy_from_slice(&header);
        self.0.read_exact(&mut bytes[HEADER_BYTES..]).ok()?;
        let expected = u32::from_le_bytes(header[4..].try_into().ok()?);
        (checksum(&bytes[HEADER_BYTES..]) == expected).then_some(bytes)
    }
}

/// FNV-1a. It detects torn and damaged records; nothing here needs to resist tampering.
fn checksum(bytes: &[u8]) -> u32 {
    bytes.iter().fold(0x811C_9DC5, |hash, byte| (hash ^ u32::from(*byte)).wrapping_mul(0x0100_0193))
}

fn encode(record: &PingRecord) -> Vec<u8> {
    let id = record.driver.as_bytes();
    let id = &id[..id.len().min(MAX_PAYLOAD_BYTES - FIXED_PAYLOAD_BYTES)];
    let mut bytes = Vec::with_capacity(HEADER_BYTES + FIXED_PAYLOAD_BYTES + id.len());
    bytes.extend_from_slice(&[0; HEADER_BYTES]);
    bytes.extend_from_slice(&record.reported_at_ms.to_le_bytes());
    bytes.extend_from_slice(&record.received_at_ms.to_le_bytes());
    bytes.extend_from_slice(&record.position.lat.to_le_bytes());
    bytes.extend_from_slice(&record.position.lng.to_le_bytes());
    bytes.extend_from_slice(id);
    let length = (bytes.len() - HEADER_BYTES) as u32;
    let sum = checksum(&bytes[HEADER_BYTES..]);
    bytes[..4].copy_from_slice(&length.to_le_bytes());
    bytes[4..HEADER_BYTES].copy_from_slice(&sum.to_le_bytes());
    bytes
}

fn decode(payload: &[u8]) -> Option<PingRecord> {
    let field = |at: usize| -> Option<[u8; 8]> { payload.get(at..at + 8)?.try_into().ok() };
    Some(PingRecord {
        reported_at_ms: i64::from_le_bytes(field(0)?),
        received_at_ms: i64::from_le_bytes(field(8)?),
        position: Position { lat: f64::from_le_bytes(field(16)?), lng: f64::from_le_bytes(field(24)?) },
        driver: String::from_utf8(payload.get(FIXED_PAYLOAD_BYTES..)?.to_vec()).ok()?,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU32;

    use super::*;

    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            static NEXT: AtomicU32 = AtomicU32::new(0);
            let id = NEXT.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!("ping-log-test-{}-{id}", std::process::id()));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn record(index: i64) -> PingRecord {
        PingRecord {
            driver: format!("d{index}"),
            reported_at_ms: index,
            received_at_ms: index + 5,
            position: Position { lat: 40.7 + index as f64 * 1e-4, lng: -74.0 },
        }
    }

    fn records(range: std::ops::Range<i64>) -> Vec<PingRecord> {
        range.map(record).collect()
    }

    fn write_log(config: &Config, range: std::ops::Range<i64>) {
        let log = PingLog::open(config.clone()).unwrap();
        for index in range {
            assert!(log.append(&record(index)));
        }
    }

    #[test]
    fn records_survive_a_clean_restart_in_order() {
        let dir = TempDir::new();
        let config = Config::new(dir.0.clone());
        write_log(&config, 0..1000);
        write_log(&config, 1000..1500);
        assert_eq!(read_all(&dir.0).unwrap(), records(0..1500));
    }

    #[test]
    fn closed_segments_are_compressed_and_no_open_data_is_left_after_shutdown() {
        let dir = TempDir::new();
        write_log(&Config::new(dir.0.clone()), 0..20_000);
        let paths = segment_paths(&dir.0).unwrap();
        assert!(paths.iter().all(|path| has_kind(path, CLOSED)), "{paths:?}");
        let compressed: u64 = paths.iter().map(|path| fs::metadata(path).unwrap().len()).sum();
        let raw: u64 = records(0..20_000).iter().map(|record| encode(record).len() as u64).sum();
        assert!(compressed < raw / 2, "{compressed} of {raw} bytes");
    }

    #[test]
    fn segments_rotate_at_the_size_limit_and_lose_nothing() {
        let dir = TempDir::new();
        let config = Config { segment_bytes: 4096, ..Config::new(dir.0.clone()) };
        write_log(&config, 0..2000);
        assert!(segment_paths(&dir.0).unwrap().len() > 3);
        assert_eq!(read_all(&dir.0).unwrap(), records(0..2000));
    }

    #[test]
    fn a_torn_final_record_costs_only_that_record() {
        let dir = TempDir::new();
        let path = dir.0.join(format!("{:020}-0000.{OPEN}", 1));
        let mut bytes: Vec<u8> = records(0..10).iter().flat_map(encode).collect();
        bytes.truncate(bytes.len() - 3);
        fs::write(&path, bytes).unwrap();
        assert_eq!(read_all(&dir.0).unwrap(), records(0..9));

        // Reopening compresses what's left of it.
        drop(PingLog::open(Config::new(dir.0.clone())).unwrap());
        assert!(!path.exists());
        assert_eq!(read_all(&dir.0).unwrap(), records(0..9));
    }

    #[test]
    fn a_damaged_record_is_never_returned() {
        let dir = TempDir::new();
        let mut bytes: Vec<u8> = records(0..5).iter().flat_map(encode).collect();
        let second = encode(&record(0)).len();
        bytes[second + HEADER_BYTES] ^= 0xFF;
        fs::write(dir.0.join(format!("{:020}-0000.{OPEN}", 1)), bytes).unwrap();
        assert_eq!(read_all(&dir.0).unwrap(), records(0..1));
    }

    #[test]
    fn a_crash_during_compression_keeps_the_open_segment() {
        let dir = TempDir::new();
        let path = dir.0.join(format!("{:020}-0000.{OPEN}", 1));
        fs::write(&path, records(0..5).iter().flat_map(encode).collect::<Vec<u8>>()).unwrap();
        fs::write(path.with_extension(COMPRESSING), b"partial").unwrap();
        drop(PingLog::open(Config::new(dir.0.clone())).unwrap());
        assert_eq!(read_all(&dir.0).unwrap(), records(0..5));
    }

    #[test]
    fn a_crash_between_rename_and_delete_does_not_duplicate() {
        let dir = TempDir::new();
        let path = dir.0.join(format!("{:020}-0000.{OPEN}", 1));
        let bytes: Vec<u8> = records(0..5).iter().flat_map(encode).collect();
        fs::write(&path, &bytes).unwrap();
        let copy = dir.0.join("copy");
        fs::write(&copy, &bytes).unwrap();
        compress(&copy).unwrap_or(());
        fs::rename(copy.with_extension(CLOSED), path.with_extension(CLOSED)).unwrap();
        drop(PingLog::open(Config::new(dir.0.clone())).unwrap());
        assert_eq!(read_all(&dir.0).unwrap(), records(0..5));
    }

    fn write_segment(dir: &Path, start_ms: u128, bytes: usize, kind: &str) -> PathBuf {
        let path = dir.join(format!("{start_ms:020}-0000.{kind}"));
        fs::write(&path, vec![0; bytes]).unwrap();
        path
    }

    #[test]
    fn segments_age_out_only_once_all_their_pings_are_old() {
        let dir = TempDir::new();
        let old = write_segment(&dir.0, 1_000, 10, CLOSED);
        let straddling = write_segment(&dir.0, 5_000, 10, CLOSED);
        let recent = write_segment(&dir.0, 9_000, 10, OPEN);
        enforce_retention(&dir.0, Duration::from_secs(5), u64::MAX, 11_000).unwrap();
        assert!(!old.exists());
        assert!(straddling.exists(), "it may hold pings newer than the cutoff");
        assert!(recent.exists());
    }

    #[test]
    fn the_size_cap_deletes_oldest_first_and_never_the_open_segment() {
        let dir = TempDir::new();
        let oldest = write_segment(&dir.0, 1_000, 100, CLOSED);
        let middle = write_segment(&dir.0, 2_000, 100, CLOSED);
        let newest = write_segment(&dir.0, 3_000, 100, CLOSED);
        let open = write_segment(&dir.0, 4_000, 100, OPEN);
        enforce_retention(&dir.0, Duration::from_secs(1_000_000), 250, 5_000).unwrap();
        assert!(!oldest.exists() && !middle.exists());
        assert!(newest.exists() && open.exists());
    }

    #[test]
    fn a_ping_log_keeps_disk_use_within_its_cap_while_running() {
        let dir = TempDir::new();
        let config = Config { segment_bytes: 8 * 1024, max_bytes: 40 * 1024, ..Config::new(dir.0.clone()) };
        write_log(&config, 0..30_000);
        let total: u64 =
            segment_paths(&dir.0).unwrap().iter().map(|path| fs::metadata(path).unwrap().len()).sum();
        assert!(total <= config.max_bytes, "{total} bytes");
        assert!(total > 0);
    }
}
