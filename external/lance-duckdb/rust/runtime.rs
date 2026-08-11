use std::future::Future;
use std::io;
use std::sync::OnceLock;

use tokio::runtime::{Builder, Handle, Runtime};

static RUNTIME: OnceLock<Result<Runtime, io::Error>> = OnceLock::new();

fn configured_worker_threads() -> usize {
    // Vane's Ray workers set VANE_LANCE_WORKER_CPUS to their admitted CPU
    // capacity.  OMP_NUM_THREADS is Ray's process-level fallback.  Refuse zero
    // and malformed values so Tokio never falls back to an unbounded or invalid
    // configuration.
    for name in ["VANE_LANCE_WORKER_CPUS", "OMP_NUM_THREADS"] {
        if let Some(value) = std::env::var_os(name) {
            if let Ok(value) = value.to_string_lossy().parse::<usize>() {
                if value > 0 {
                    return value;
                }
            }
        }
    }
    std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
        .max(1)
}

fn build_runtime() -> Result<Runtime, io::Error> {
    Builder::new_multi_thread()
        .worker_threads(configured_worker_threads())
        .thread_name("vane-lance")
        .enable_all()
        .build()
}

pub fn runtime() -> Result<&'static Runtime, io::Error> {
    match RUNTIME.get_or_init(build_runtime) {
        Ok(rt) => Ok(rt),
        Err(err) => Err(io::Error::new(err.kind(), err.to_string())),
    }
}

pub fn initialized_runtime() -> Option<&'static Runtime> {
    RUNTIME.get()?.as_ref().ok()
}

pub fn handle() -> Result<Handle, io::Error> {
    Ok(runtime()?.handle().clone())
}

pub fn block_on<F: Future>(future: F) -> Result<F::Output, io::Error> {
    Ok(runtime()?.block_on(future))
}
