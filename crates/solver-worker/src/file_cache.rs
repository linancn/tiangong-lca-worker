//! Best-effort, per-file cache advice for large sequential temporary artifacts.
//!
//! Advice failures are telemetry only. They must never change artifact correctness
//! or make a supported workload fail.

use std::fs::File;

/// Advises the operating system that `file` will be accessed sequentially.
///
/// Linux uses `POSIX_FADV_SEQUENTIAL`. macOS enables `F_NOCACHE` on this file
/// descriptor so subsequent I/O does not populate the unified buffer cache.
/// Other platforms intentionally do nothing.
pub fn advise_sequential_access(file: &File) {
    if let Err(error) = platform::advise_sequential_access(file) {
        tracing::debug!(
            operation = "advise_sequential_access",
            error = %error,
            "file cache advice was not applied"
        );
    }
}

/// Best-effort release of clean cached pages after the caller's final file use.
///
/// On Linux this syncs file data before issuing `POSIX_FADV_DONTNEED`; dirty
/// pages may otherwise remain cached. macOS relies on the `F_NOCACHE` advice
/// installed before I/O. Other platforms intentionally do nothing.
pub fn release_file_cache(file: &File) {
    if let Err(error) = platform::release_file_cache(file) {
        tracing::debug!(
            operation = "release_file_cache",
            error = %error,
            "file cache release advice was not applied"
        );
    }
}

#[cfg(target_os = "linux")]
mod platform {
    use std::{fs::File, io, os::fd::AsRawFd};

    pub(super) fn advise_sequential_access(file: &File) -> io::Result<()> {
        posix_fadvise(file, libc::POSIX_FADV_SEQUENTIAL)
    }

    pub(super) fn release_file_cache(file: &File) -> io::Result<()> {
        file.sync_data()?;
        posix_fadvise(file, libc::POSIX_FADV_DONTNEED)
    }

    fn posix_fadvise(file: &File, advice: libc::c_int) -> io::Result<()> {
        // SAFETY: `file` owns a valid descriptor for the duration of this call.
        // Offset zero and length zero request advice for the complete file.
        let error = unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, advice) };
        if error == 0 {
            Ok(())
        } else {
            Err(io::Error::from_raw_os_error(error))
        }
    }
}

#[cfg(target_os = "macos")]
mod platform {
    use std::{fs::File, io, os::fd::AsRawFd};

    pub(super) fn advise_sequential_access(file: &File) -> io::Result<()> {
        // SAFETY: `file` owns a valid descriptor for the duration of this call.
        // F_NOCACHE changes caching behavior only for this descriptor.
        let result = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1) };
        if result == -1 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    pub(super) fn release_file_cache(file: &File) -> io::Result<()> {
        file.sync_data()
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
mod platform {
    use std::fs::File;

    pub(super) const fn advise_sequential_access(
        _file: &File,
    ) -> Result<(), std::convert::Infallible> {
        Ok(())
    }

    pub(super) const fn release_file_cache(_file: &File) -> Result<(), std::convert::Infallible> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Seek, SeekFrom, Write};

    use super::{advise_sequential_access, release_file_cache};

    #[test]
    fn cache_advice_never_changes_file_contents() {
        let mut temp = tempfile::NamedTempFile::new().expect("create temporary file");
        advise_sequential_access(temp.as_file());
        temp.write_all(b"deterministic artifact bytes")
            .expect("write temporary file");
        temp.flush().expect("flush temporary file");
        release_file_cache(temp.as_file());

        temp.seek(SeekFrom::Start(0))
            .expect("rewind temporary file");
        let mut observed = Vec::new();
        temp.read_to_end(&mut observed)
            .expect("read temporary file");
        assert_eq!(observed, b"deterministic artifact bytes");
    }
}
