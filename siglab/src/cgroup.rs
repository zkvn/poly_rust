//! CPU/memory sampling from cgroup v2 files — works from inside the container itself, no
//! docker socket / host access needed (verified: `/sys/fs/cgroup/cpu.stat` and
//! `/sys/fs/cgroup/memory.current` are both readable inside the siglab image without any
//! extra privileges). Falls back to `None` gracefully outside a cgroup v2 environment
//! (e.g. running `cargo run` directly on a dev machine) rather than erroring — this is
//! monitoring, not something that should ever crash the process.

use std::time::Instant;

#[derive(Debug, Clone, Copy)]
pub struct Sample {
    pub cpu_usec: u64,
    pub mem_bytes: u64,
    pub at: Instant,
}

fn read_cpu_usec() -> Option<u64> {
    let stat = std::fs::read_to_string("/sys/fs/cgroup/cpu.stat").ok()?;
    stat.lines()
        .find_map(|l| l.strip_prefix("usage_usec "))
        .and_then(|v| v.trim().parse().ok())
}

fn read_mem_bytes() -> Option<u64> {
    std::fs::read_to_string("/sys/fs/cgroup/memory.current")
        .ok()?
        .trim()
        .parse()
        .ok()
}

pub fn sample() -> Option<Sample> {
    Some(Sample {
        cpu_usec: read_cpu_usec()?,
        mem_bytes: read_mem_bytes()?,
        at: Instant::now(),
    })
}

/// Average CPU usage between two samples, as a percentage of one core (100% = one core
/// fully busy for the whole interval) — same convention as `docker stats`' CPU% column.
pub fn cpu_percent(prev: &Sample, now: &Sample) -> f64 {
    let elapsed_usec = now.at.duration_since(prev.at).as_micros() as f64;
    if elapsed_usec <= 0.0 {
        return 0.0;
    }
    let cpu_delta_usec = now.cpu_usec.saturating_sub(prev.cpu_usec) as f64;
    100.0 * cpu_delta_usec / elapsed_usec
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn cpu_percent_half_core_busy() {
        let prev = Sample {
            cpu_usec: 0,
            mem_bytes: 0,
            at: Instant::now(),
        };
        let now = Sample {
            cpu_usec: 500_000, // 0.5s of CPU time
            mem_bytes: 0,
            at: prev.at + Duration::from_secs(1), // over 1s wall time
        };
        let pct = cpu_percent(&prev, &now);
        assert!((pct - 50.0).abs() < 0.01);
    }

    #[test]
    fn cpu_percent_zero_elapsed_is_zero_not_nan() {
        let s = Sample {
            cpu_usec: 100,
            mem_bytes: 0,
            at: Instant::now(),
        };
        assert_eq!(cpu_percent(&s, &s), 0.0);
    }
}
