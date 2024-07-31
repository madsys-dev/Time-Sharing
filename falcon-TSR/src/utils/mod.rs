use std::time::{Duration, Instant, SystemTime};

pub mod executor;
pub mod file;
pub mod io;
pub mod macros;
pub mod persist;
pub mod message;

pub fn wait_for_nanos(nanos: u32) {
    if nanos <= 100 {
        return;
    }
    let start: Instant = Instant::now();
    if nanos <= 200 {
        return;
    }
    let duration: Duration = std::time::Duration::new(0, nanos-100);
    loop {
        let end = Instant::now();
        if end.duration_since(start).ge(&duration) {
            break;
        }
    }
}
