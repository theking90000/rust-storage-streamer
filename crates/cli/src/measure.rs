use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_stream::try_stream;
use frame_streamer::{
    EncryptedByteStream, EncryptedBytesDownloadBackend, ObjectMeta, SignedUrl, UrlTicket,
};
use futures_util::StreamExt;
use reqwest::header::RANGE;

pub struct MeasureBackend {
    client: reqwest::Client,
    metrics: Metrics,
    frame_size: u64,
    target_bps: f64,
    window_frames: Arc<AtomicUsize>,
    resolves: Arc<Mutex<HashMap<String, Duration>>>,
}

impl MeasureBackend {
    pub fn new(
        client: reqwest::Client,
        directory: impl Into<PathBuf>,
        frame_size: usize,
        target_bps: f64,
    ) -> Self {
        Self {
            client,
            metrics: Metrics::new(directory.into()),
            frame_size: frame_size as u64,
            target_bps,
            window_frames: Arc::new(AtomicUsize::new(0)),
            resolves: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn set_window_frames(&self, frames: usize) {
        self.window_frames.store(frames, Ordering::Relaxed);
    }

    pub fn dropped_samples(&self) -> usize {
        self.metrics.dropped()
    }
}

impl EncryptedBytesDownloadBackend for MeasureBackend {
    fn resolve_url(&self, object: &ObjectMeta) -> UrlTicket {
        let id = object.id.as_str().to_owned();
        let url = object.uri.clone();
        let resolves = self.resolves.clone();
        Box::pin(async move {
            let started = Instant::now();
            resolves.lock().unwrap().insert(id, started.elapsed());
            Ok(SignedUrl::new(url, None))
        })
    }

    fn download(
        &self,
        object: &ObjectMeta,
        url: SignedUrl,
        physical_bytes: Range<u64>,
    ) -> EncryptedByteStream {
        let client = self.client.clone();
        let id = object.id.as_str().to_owned();
        let host = reqwest::Url::parse(url.as_str())
            .ok()
            .and_then(|url| url.host_str().map(str::to_owned))
            .unwrap_or_default();
        let metrics = self.metrics.clone();
        let frame_size = self.frame_size;
        let target_bps = self.target_bps;
        let window_frames = self.window_frames.load(Ordering::Relaxed);
        let url_resolve = self.resolves.lock().unwrap().remove(&id);

        Box::pin(try_stream! {
            let expected = physical_bytes.end - physical_bytes.start;
            let range = format!("bytes={}-{}", physical_bytes.start, physical_bytes.end - 1);
            println!("GET {id} {range}");
            let started = Instant::now();
            let mut sample = SampleGuard::new(metrics, TransferSample {
                timestamp: unix_millis(),
                session_id: session_id(),
                object_id: id.clone(),
                host,
                range_start: physical_bytes.start,
                range_end: physical_bytes.end,
                frame_count: expected / frame_size,
                target_bps,
                window_frames,
                cache_status: String::new(),
                age: String::new(),
                server_timing: String::new(),
                url_resolve_ms: url_resolve.map(milliseconds),
                headers_ttfb_ms: None,
                first_byte_ms: None,
                body_ms: None,
                total_ms: 0.0,
                bytes_received: 0,
                effective_body_bps: None,
                result: "cancelled".into(),
            }, started);

            let response = match client.get(url.as_str()).header(RANGE, range).send().await {
                Ok(response) => response,
                Err(error) => {
                    sample.fail(format!("request: {error}"));
                    Err(error)?
                }
            };
            sample.headers(response.headers(), started.elapsed());
            let response = match response.error_for_status() {
                Ok(response) => response,
                Err(error) => {
                    sample.fail(format!("http: {error}"));
                    Err(error)?
                }
            };
            let full_object = response.status() == reqwest::StatusCode::OK
                && physical_bytes.start == 0
                && response.content_length() == Some(expected);
            if response.status() != reqwest::StatusCode::PARTIAL_CONTENT && !full_object {
                let error = format!("{id}: server ignored the Range request");
                sample.fail(error.clone());
                Err(std::io::Error::other(error))?;
            }

            let mut body = response.bytes_stream();
            while let Some(chunk) = body.next().await {
                match chunk {
                    Ok(chunk) => {
                        sample.chunk(chunk.len(), started.elapsed());
                        yield chunk;
                    }
                    Err(error) => {
                        sample.fail(format!("body: {error}"));
                        Err(error)?;
                    }
                }
            }
            sample.complete();
        })
    }
}

#[derive(Clone)]
struct Metrics {
    inner: Arc<MetricsInner>,
}

struct MetricsInner {
    sender: Option<mpsc::SyncSender<TransferSample>>,
    dropped: AtomicUsize,
    writer: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Metrics {
    fn new(directory: PathBuf) -> Self {
        let (sender, receiver) = mpsc::sync_channel(1024);
        let writer = std::thread::spawn(move || write_samples(&directory, receiver));
        Self {
            inner: Arc::new(MetricsInner {
                sender: Some(sender),
                dropped: AtomicUsize::new(0),
                writer: Mutex::new(Some(writer)),
            }),
        }
    }

    fn send(&self, sample: TransferSample) {
        if self
            .inner
            .sender
            .as_ref()
            .unwrap()
            .try_send(sample)
            .is_err()
        {
            self.inner.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn dropped(&self) -> usize {
        self.inner.dropped.load(Ordering::Relaxed)
    }
}

impl Drop for MetricsInner {
    fn drop(&mut self) {
        self.sender.take();
        if let Some(writer) = self.writer.get_mut().unwrap().take() {
            let _ = writer.join();
        }
    }
}

struct SampleGuard {
    metrics: Metrics,
    sample: Option<TransferSample>,
    started: Instant,
    first_byte_at: Option<Instant>,
}

impl SampleGuard {
    fn new(metrics: Metrics, sample: TransferSample, started: Instant) -> Self {
        Self {
            metrics,
            sample: Some(sample),
            started,
            first_byte_at: None,
        }
    }

    fn headers(&mut self, headers: &reqwest::header::HeaderMap, elapsed: Duration) {
        let sample = self.sample.as_mut().unwrap();
        sample.headers_ttfb_ms = Some(milliseconds(elapsed));
        sample.cache_status = header(headers, "cf-cache-status");
        sample.age = header(headers, "age");
        sample.server_timing = header(headers, "server-timing");
    }

    fn chunk(&mut self, bytes: usize, elapsed: Duration) {
        let now = Instant::now();
        if self.first_byte_at.is_none() {
            self.first_byte_at = Some(now);
            self.sample.as_mut().unwrap().first_byte_ms = Some(milliseconds(elapsed));
        }
        self.sample.as_mut().unwrap().bytes_received += bytes as u64;
    }

    fn fail(&mut self, result: String) {
        self.sample.as_mut().unwrap().result = result;
    }

    fn complete(&mut self) {
        self.sample.as_mut().unwrap().result = "ok".into();
        self.emit();
    }

    fn emit(&mut self) {
        let Some(mut sample) = self.sample.take() else {
            return;
        };
        let completed = Instant::now();
        sample.total_ms = milliseconds(completed.duration_since(self.started));
        if let Some(first) = self.first_byte_at {
            let body = completed.duration_since(first);
            sample.body_ms = Some(milliseconds(body));
            if !body.is_zero() {
                sample.effective_body_bps = Some(sample.bytes_received as f64 / body.as_secs_f64());
            }
        }
        self.metrics.send(sample);
    }
}

impl Drop for SampleGuard {
    fn drop(&mut self) {
        self.emit();
    }
}

struct TransferSample {
    timestamp: u128,
    session_id: String,
    object_id: String,
    host: String,
    range_start: u64,
    range_end: u64,
    frame_count: u64,
    target_bps: f64,
    window_frames: usize,
    cache_status: String,
    age: String,
    server_timing: String,
    url_resolve_ms: Option<f64>,
    headers_ttfb_ms: Option<f64>,
    first_byte_ms: Option<f64>,
    body_ms: Option<f64>,
    total_ms: f64,
    bytes_received: u64,
    effective_body_bps: Option<f64>,
    result: String,
}

fn write_samples(directory: &Path, receiver: mpsc::Receiver<TransferSample>) {
    let mut current_day = String::new();
    let mut writer: Option<BufWriter<File>> = None;
    for sample in receiver {
        let day = civil_date((sample.timestamp / 86_400_000) as i64);
        if day != current_day {
            current_day = day;
            writer = open_csv(directory, &current_day).ok();
        }
        if let Some(file) = writer.as_mut()
            && writeln!(file, "{}", sample.csv())
                .and_then(|_| file.flush())
                .is_err()
        {
            eprintln!("metrics: failed to write CSV");
            writer = None;
        }
    }
}

fn open_csv(directory: &Path, day: &str) -> std::io::Result<BufWriter<File>> {
    std::fs::create_dir_all(directory)?;
    let path = directory.join(format!("transfers-{day}.csv"));
    let new = !path.exists();
    let file = OpenOptions::new().create(true).append(true).open(path)?;
    let mut writer = BufWriter::new(file);
    if new {
        writeln!(
            writer,
            "timestamp,session_id,object_id,host,range_start,range_end,frame_count,target_bps,window_frames,cache_status,age,server_timing,url_resolve_ms,headers_ttfb_ms,first_byte_ms,body_ms,total_ms,bytes_received,effective_body_bps,result"
        )?;
    }
    Ok(writer)
}

impl TransferSample {
    fn csv(&self) -> String {
        [
            self.timestamp.to_string(),
            csv(&self.session_id),
            csv(&self.object_id),
            csv(&self.host),
            self.range_start.to_string(),
            self.range_end.to_string(),
            self.frame_count.to_string(),
            format!("{:.3}", self.target_bps),
            self.window_frames.to_string(),
            csv(&self.cache_status),
            csv(&self.age),
            csv(&self.server_timing),
            number(self.url_resolve_ms),
            number(self.headers_ttfb_ms),
            number(self.first_byte_ms),
            number(self.body_ms),
            format!("{:.3}", self.total_ms),
            self.bytes_received.to_string(),
            number(self.effective_body_bps),
            csv(&self.result),
        ]
        .join(",")
    }
}

fn header(headers: &reqwest::header::HeaderMap, name: &str) -> String {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned()
}

fn csv(value: &str) -> String {
    if value.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_owned()
    }
}

fn number(value: Option<f64>) -> String {
    value.map(|value| format!("{value:.3}")).unwrap_or_default()
}

fn milliseconds(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn unix_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn session_id() -> String {
    static ID: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    ID.get_or_init(|| format!("{}-{}", unix_millis(), std::process::id()))
        .clone()
}

// Howard Hinnant's civil-from-days algorithm; avoids a date dependency for one filename.
fn civil_date(days_since_epoch: i64) -> String {
    let z = days_since_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let day_of_era = z - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    format!("{year:04}-{month:02}-{day:02}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_dates_and_csv_fields() {
        assert_eq!(civil_date(0), "1970-01-01");
        assert_eq!(civil_date(20_622), "2026-06-18");
        assert_eq!(csv("a,\"b\""), "\"a,\"\"b\"\"\"");
    }
}
