#[cfg(feature = "metrics")]
use metrics::counter;

#[cfg(feature = "metrics")]
#[inline]
pub fn record_bytes_written(handle_name: &str, total_bytes: u64, backend_name: String) {
    counter!(format!("{}_bytes_written", handle_name), total_bytes, "backend" => backend_name);
}

#[cfg(feature = "metrics")]
#[inline]
pub fn record_bytes_read(handle_name: &str, total_bytes: u64, backend_name: String) {
    counter!(format!("{}_bytes_read", handle_name), total_bytes, "backend" => backend_name);
}
