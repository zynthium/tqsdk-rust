//! File publication primitives shared by history containers. Callers own the
//! stable companion lock; publication never replaces that lock's inode.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::{DataError, Result};

pub(crate) struct Candidate {
    pub file: File,
    path: PathBuf,
    published: bool,
}

impl Candidate {
    pub fn create(destination: &Path) -> Result<Self> {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let parent = destination
            .parent()
            .ok_or(DataError::InvalidState("missing cache parent"))?;
        fs::create_dir_all(parent)?;
        let path = destination.with_extension(format!(
            "tqbn.cow-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos(),
            NEXT.fetch_add(1, Ordering::Relaxed),
        ));
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)?;
        Ok(Self {
            file,
            path,
            published: false,
        })
    }

    pub fn publish(mut self, destination: &Path) -> Result<()> {
        self.file.sync_all()?;
        fs::rename(&self.path, destination)?;
        self.published = true;
        sync_parent(destination)
    }
}

impl Drop for Candidate {
    fn drop(&mut self) {
        if !self.published {
            // The guard exists only after create_new succeeded.
            let _ = fs::remove_file(&self.path);
        }
    }
}

pub(crate) fn sync_parent(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .ok_or(DataError::InvalidState("missing cache parent"))?;
    #[cfg(unix)]
    File::open(parent)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = parent;
    Ok(())
}

pub(crate) fn detach(path: &Path, input: &mut File, append_only: bool) -> Result<()> {
    validate_open_path(path, input)?;
    if link_count(input)? <= 1 {
        return Ok(());
    }
    let metadata = input.metadata()?;
    let mut candidate = Candidate::create(path)?;
    input.seek(SeekFrom::Start(0))?;
    let copied = std::io::copy(&mut (&mut *input).take(metadata.len()), &mut candidate.file)?;
    if copied != metadata.len() {
        return Err(DataError::InvalidResponse(
            "cache COW source truncated".into(),
        ));
    }
    candidate.file.set_permissions(metadata.permissions())?;
    candidate.publish(path)?;
    *input = OpenOptions::new()
        .read(true)
        .write(!append_only)
        .append(append_only)
        .open(path)?;
    Ok(())
}

/// Call under the stable companion lock. Reject aliases before mutating an
/// opened file; all cooperating publishers must hold that same lock.
pub(crate) fn validate_open_path(path: &Path, input: &File) -> Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() || !input.metadata()?.is_file() {
        return Err(DataError::InvalidState(
            "cache mutation requires a regular non-symlink file",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let opened = input.metadata()?;
        if (metadata.dev(), metadata.ino()) != (opened.dev(), opened.ino()) {
            return Err(DataError::InvalidState(
                "cache path no longer names opened file",
            ));
        }
    }
    #[cfg(windows)]
    {
        let current = windows_file_information(&File::open(path)?)?;
        let opened = windows_file_information(input)?;
        if (
            current.dwVolumeSerialNumber,
            current.nFileIndexHigh,
            current.nFileIndexLow,
        ) != (
            opened.dwVolumeSerialNumber,
            opened.nFileIndexHigh,
            opened.nFileIndexLow,
        ) {
            return Err(DataError::InvalidState(
                "cache path no longer names opened file",
            ));
        }
    }
    Ok(())
}

#[cfg(unix)]
pub(crate) fn link_count(file: &File) -> Result<u64> {
    use std::os::unix::fs::MetadataExt;
    Ok(file.metadata()?.nlink())
}

#[cfg(windows)]
pub(crate) fn link_count(file: &File) -> Result<u64> {
    Ok(u64::from(windows_file_information(file)?.nNumberOfLinks))
}

#[cfg(windows)]
#[allow(unsafe_code)]
fn windows_file_information(
    file: &File,
) -> Result<windows_sys::Win32::Storage::FileSystem::BY_HANDLE_FILE_INFORMATION> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        BY_HANDLE_FILE_INFORMATION, GetFileInformationByHandle,
    };
    let mut output = std::mem::MaybeUninit::<BY_HANDLE_FILE_INFORMATION>::uninit();
    // SAFETY: the handle is live and the output is correctly sized/aligned.
    if unsafe { GetFileInformationByHandle(file.as_raw_handle(), output.as_mut_ptr()) } == 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    // SAFETY: a successful call initializes the complete output.
    Ok(unsafe { output.assume_init() })
}

#[cfg(not(any(unix, windows)))]
pub(crate) fn link_count(_file: &File) -> Result<u64> {
    Err(DataError::InvalidState(
        "cache mutation requires hardlink-count support",
    ))
}
