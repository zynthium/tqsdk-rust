//! Cross-process admission for CLI fills, independent of the cache root.
use std::fs::{File, OpenOptions};
use std::time::Duration;

use fs2::FileExt;

use crate::CliError;

pub(crate) async fn acquire(wait: Option<u64>) -> Result<File, CliError> {
    #[cfg(unix)]
    let path = {
        // No credentials or cache paths enter the lock identity.
        let user = unsafe { libc::geteuid() };
        std::path::PathBuf::from(format!("/tmp/tqsdk-cache-fill-{user}.lock"))
    };
    #[cfg(not(unix))]
    let path = std::env::var_os("LOCALAPPDATA")
        .map(std::path::PathBuf::from)
        .ok_or_else(|| {
            CliError::Usage("LOCALAPPDATA is required for per-user fill admission".into())
        })?
        .join("tqsdk-cache-fill.lock");
    acquire_path(path, wait).await
}

async fn acquire_path(path: std::path::PathBuf, wait: Option<u64>) -> Result<File, CliError> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    let file = options.open(&path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(CliError::Usage(
            "fill admission lock must be a regular file".into(),
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.uid() != unsafe { libc::geteuid() }
            || metadata.nlink() != 1
            || metadata.mode() & 0o077 != 0
        {
            return Err(CliError::Usage(
                "fill admission lock has unsafe ownership or permissions".into(),
            ));
        }
    }
    let started = tokio::time::Instant::now();
    loop {
        match file.try_lock_exclusive() {
            Ok(()) => return Ok(file),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                if started.elapsed() >= Duration::from_secs(wait.unwrap_or(0)) {
                    return Err(tqsdk_data::DataError::CacheBusy {
                        cache_dir: path,
                        operation: "global remote fill admission",
                    }
                    .into());
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(error) => return Err(error.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn admission_serializes_different_roots_and_releases_after_drop() {
        let path =
            std::env::temp_dir().join(format!("tqsdk-fill-guard-test-{}", std::process::id()));
        let first = acquire_path(path.clone(), None).await.unwrap();
        let error = acquire_path(path.clone(), None).await.unwrap_err();
        assert_eq!(error.exit_code(), 75);
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "fill_guard::tests::child_lock_probe"])
            .env("TQSDK_TEST_FILL_LOCK", &path)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "child must observe the held admission lock"
        );
        drop(first);
        let second = acquire_path(path.clone(), None).await.unwrap();
        drop(second);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn child_lock_probe() {
        let Some(path) = std::env::var_os("TQSDK_TEST_FILL_LOCK") else {
            return;
        };
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        assert_eq!(
            file.try_lock_exclusive().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
    }
}
