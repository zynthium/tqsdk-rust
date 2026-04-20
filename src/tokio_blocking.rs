use std::{
    future::Future,
    sync::{Mutex, OnceLock},
};

use tokio::runtime::Runtime;

use crate::Result;

pub(crate) fn block_on_with_shared_runtime<F, T, Build>(
    runtime: &'static OnceLock<Mutex<Runtime>>,
    build_runtime: Build,
    future: F,
) -> Result<T>
where
    F: Future<Output = Result<T>>,
    Build: FnOnce() -> Result<Runtime>,
{
    let runtime = if let Some(runtime) = runtime.get() {
        runtime
    } else {
        let _ = runtime.set(Mutex::new(build_runtime()?));
        runtime
            .get()
            .expect("shared tokio runtime should be initialized")
    };
    runtime
        .lock()
        .expect("shared tokio runtime mutex poisoned")
        .block_on(future)
}
