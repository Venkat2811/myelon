use std::path::PathBuf;

pub struct PlatformInfo {
    pub target_dir: PathBuf,
}

impl PlatformInfo {
    pub fn detect(target_dir: PathBuf) -> Self {
        Self { target_dir }
    }

    pub fn is_macos(&self) -> bool {
        cfg!(target_os = "macos")
    }

    pub fn ipc_dir(&self) -> &'static str {
        if self.is_macos() {
            "/tmp"
        } else {
            "/dev/shm"
        }
    }

    /// Resolve the path to a compiled binary in the target directory.
    pub fn binary_path(&self, profile: &str, name: &str) -> PathBuf {
        self.target_dir.join(profile).join(name)
    }

    /// Aeron library environment variable and search paths for rusteron.
    pub fn aeron_env(&self) -> Vec<(String, String)> {
        let env_key = if self.is_macos() {
            "DYLD_LIBRARY_PATH"
        } else {
            "LD_LIBRARY_PATH"
        };

        let glob_pattern = if self.is_macos() {
            "libaeron*.dylib"
        } else {
            "libaeron*.so"
        };

        let lib_dirs = find_aeron_lib_dirs(&self.target_dir, glob_pattern);
        if lib_dirs.is_empty() {
            return vec![];
        }

        let current = std::env::var(env_key).unwrap_or_default();
        let value = if current.is_empty() {
            lib_dirs
        } else {
            format!("{lib_dirs}:{current}")
        };
        vec![(env_key.to_string(), value)]
    }
}

fn find_aeron_lib_dirs(target_dir: &std::path::Path, glob_pattern: &str) -> String {
    // Search in target/{profile}/build/**/out/ for aeron libraries
    let build_dir = target_dir.join("competitive").join("build");
    if !build_dir.exists() {
        return String::new();
    }

    let mut dirs = Vec::new();
    if let Ok(entries) = std::fs::read_dir(&build_dir) {
        for entry in entries.flatten() {
            let out_dir = entry.path().join("out");
            if out_dir.is_dir() {
                if let Ok(files) = std::fs::read_dir(&out_dir) {
                    for file in files.flatten() {
                        let name = file.file_name().to_string_lossy().to_string();
                        if matches_glob(&name, glob_pattern) {
                            dirs.push(out_dir.display().to_string());
                            break;
                        }
                    }
                }
            }
        }
    }
    dirs.join(":")
}

fn matches_glob(name: &str, pattern: &str) -> bool {
    if let Some(prefix) = pattern.strip_suffix('*') {
        name.starts_with(prefix)
    } else if let Some((prefix, suffix)) = pattern.split_once('*') {
        name.starts_with(prefix) && name.ends_with(suffix)
    } else {
        name == pattern
    }
}
