//! Python workers' locks: what a worker installs, how large it is on a platform, and what
//! combining workers into one environment would change. MageML runs uv to resolve (the
//! `Resolver` of WORKERS_V2.md); this module reads what uv wrote, so the checks are exact
//! and cost no network.
//!
//! A worker's size on a platform is the sum of the wheels its lock installs there: for each
//! package, the wheel whose tags fit the platform and the interpreter, or its sdist when no
//! wheel does (a build, sized by its source). uv.lock records each file's size.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// A platform a service image or a machine runs on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Platform {
    #[serde(rename = "linux-x86_64")]
    LinuxX86_64,
    #[serde(rename = "linux-aarch64")]
    LinuxAarch64,
    #[serde(rename = "macos-arm64")]
    MacosArm64,
}

impl Platform {
    /// Whether a wheel's platform tag runs here. Linux images use glibc (manylinux).
    fn fits(self, tag: &str) -> bool {
        if tag == "any" {
            return true;
        }
        match self {
            Platform::LinuxX86_64 => {
                (tag.starts_with("manylinux") || tag.starts_with("linux"))
                    && tag.ends_with("x86_64")
            }
            Platform::LinuxAarch64 => {
                (tag.starts_with("manylinux") || tag.starts_with("linux"))
                    && tag.ends_with("aarch64")
            }
            Platform::MacosArm64 => {
                tag.starts_with("macosx") && (tag.ends_with("arm64") || tag.ends_with("universal2"))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LockedPackage {
    pub name: String,
    pub version: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct LockFile {
    #[serde(default)]
    package: Vec<RawPackage>,
}

#[derive(Debug, Clone, Deserialize)]
struct RawPackage {
    name: String,
    #[serde(default)]
    version: Option<String>,
    #[serde(default)]
    source: Option<toml::Value>,
    #[serde(default)]
    sdist: Option<RawFile>,
    #[serde(default)]
    wheels: Vec<RawFile>,
}

#[derive(Debug, Clone, Deserialize)]
struct RawFile {
    #[serde(default)]
    url: Option<String>,
    #[serde(default)]
    filename: Option<String>,
    #[serde(default)]
    size: Option<u64>,
}

impl RawFile {
    fn name(&self) -> Option<&str> {
        self.filename
            .as_deref()
            .or_else(|| self.url.as_deref().and_then(|u| u.rsplit('/').next()))
    }
}

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum LockError {
    #[error("uv.lock cannot be read: {0}")]
    Parse(String),
}

/// A worker's uv.lock as read.
#[derive(Debug, Clone)]
pub struct Lock {
    packages: Vec<RawPackage>,
}

/// The installed size of a worker on a platform, and the packages whose size is a guess.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Size {
    pub bytes: u64,
    /// Packages with no wheel for the platform: their sdist's size counts, a build.
    pub built: Vec<String>,
}

/// A package whose version differs between a worker's lock and a combined resolution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Change {
    pub package: String,
    pub from: String,
    pub to: String,
}

impl Lock {
    pub fn parse(text: &str) -> Result<Lock, LockError> {
        let file: LockFile = toml::from_str(text).map_err(|e| LockError::Parse(e.to_string()))?;
        Ok(Lock {
            packages: file.package,
        })
    }

    /// The locked packages, the project itself (a virtual or editable source) left out.
    pub fn packages(&self) -> Vec<LockedPackage> {
        self.packages
            .iter()
            .filter(|p| !is_project(p))
            .filter_map(|p| {
                Some(LockedPackage {
                    name: normalize(&p.name),
                    version: p.version.clone()?,
                })
            })
            .collect()
    }

    /// What it installs on the platform for CPython `python` ("3.12").
    pub fn size(&self, platform: Platform, python: &str) -> Size {
        size_of(self.packages.iter(), platform, python)
    }
}

/// What several locks install as one environment: a package they share at one version
/// counts once (workers combined with the same versions).
pub fn combined_size(locks: &[Lock], platform: Platform, python: &str) -> Size {
    let mut seen = std::collections::HashSet::new();
    let packages = locks
        .iter()
        .flat_map(|l| l.packages.iter())
        .filter(|p| seen.insert((normalize(&p.name), p.version.clone())));
    size_of(packages, platform, python)
}

fn size_of<'a>(
    packages: impl Iterator<Item = &'a RawPackage>,
    platform: Platform,
    python: &str,
) -> Size {
    let cp = format!("cp{}", python.replace('.', ""));
    let mut size = Size::default();
    for package in packages.filter(|p| !is_project(p)) {
        let wheel = package
            .wheels
            .iter()
            .filter(|w| w.name().is_some_and(|n| wheel_fits(n, platform, &cp)))
            .filter_map(|w| w.size)
            .max();
        match (wheel, package.sdist.as_ref().and_then(|s| s.size)) {
            (Some(bytes), _) => size.bytes += bytes,
            (None, Some(bytes)) => {
                size.bytes += bytes;
                size.built.push(normalize(&package.name));
            }
            (None, None) => {}
        }
    }
    size
}

/// The project's own entry in its lock: a virtual or editable source, not a download.
fn is_project(package: &RawPackage) -> bool {
    match &package.source {
        Some(toml::Value::Table(t)) => t.contains_key("virtual") || t.contains_key("editable"),
        _ => false,
    }
}

/// PEP 503 names: lower case, runs of -, _ and . as one -.
pub fn normalize(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    let mut dash = false;
    for c in name.chars() {
        if matches!(c, '-' | '_' | '.') {
            dash = true;
        } else {
            if dash && !out.is_empty() {
                out.push('-');
            }
            dash = false;
            out.push(c.to_ascii_lowercase());
        }
    }
    out
}

/// Whether a wheel file installs on the platform for CPython `cp` ("cp312"): its python tag
/// is that version, abi3 from an older one, or any Python 3; its platform tag fits.
fn wheel_fits(filename: &str, platform: Platform, cp: &str) -> bool {
    let stem = filename.trim_end_matches(".whl");
    let parts: Vec<&str> = stem.rsplitn(4, '-').collect();
    // name-version(-build)-python-abi-platform, read from the end.
    let [platforms, abi, pythons, ..] = parts.as_slice() else {
        return false;
    };
    let python_ok = pythons.split('.').any(|p| {
        // Tags join with "." (py2.py3): each part is one tag.
        p == cp
            || p == "py3"
            || (*abi == "abi3"
                && p.strip_prefix("cp")
                    .zip(cp.strip_prefix("cp"))
                    .is_some_and(|(a, b)| {
                        a.parse::<u32>().ok() <= b.parse::<u32>().ok() && a.starts_with('3')
                    }))
    });
    python_ok && platforms.split('.').any(|tag| platform.fits(tag))
}

/// The packages whose version a combined resolution changes, from `name==version` lines
/// (uv pip compile's output; comments, hashes and markers are ignored).
pub fn changes(lock: &Lock, resolved: &str) -> Vec<Change> {
    let pinned: BTreeMap<String, String> = resolved
        .lines()
        .filter_map(|line| {
            let line = line.split('#').next()?.split(';').next()?.trim();
            let (name, version) = line.split_once("==")?;
            Some((
                normalize(name.trim()),
                version.trim().trim_end_matches('\\').trim().to_string(),
            ))
        })
        .collect();
    let mut found: Vec<Change> = lock
        .packages()
        .into_iter()
        .filter_map(|p| {
            let to = pinned.get(&p.name)?;
            (to != &p.version).then(|| Change {
                package: p.name,
                from: p.version,
                to: to.clone(),
            })
        })
        .collect();
    found.sort_by(|a, b| a.package.cmp(&b.package));
    found
}

#[cfg(test)]
mod tests {
    use super::*;

    const LOCK: &str = r#"
version = 1
requires-python = ">=3.12"

[[package]]
name = "worker"
version = "0"
source = { virtual = "." }

[[package]]
name = "pyarrow"
version = "26.0.0"
source = { registry = "https://pypi.org/simple" }
sdist = { url = "https://x/pyarrow-26.0.0.tar.gz", size = 1000 }
wheels = [
    { url = "https://x/pyarrow-26.0.0-cp312-cp312-macosx_12_0_arm64.whl", size = 36 },
    { url = "https://x/pyarrow-26.0.0-cp312-cp312-manylinux_2_28_aarch64.whl", size = 50 },
    { url = "https://x/pyarrow-26.0.0-cp312-cp312-manylinux_2_28_x86_64.whl", size = 53 },
    { url = "https://x/pyarrow-26.0.0-cp313-cp313-manylinux_2_28_x86_64.whl", size = 99 },
    { url = "https://x/pyarrow-26.0.0-cp312-cp312-musllinux_1_2_x86_64.whl", size = 57 },
]

[[package]]
name = "Six"
version = "1.16.0"
source = { registry = "https://pypi.org/simple" }
wheels = [{ url = "https://x/six-1.16.0-py2.py3-none-any.whl", size = 11 }]

[[package]]
name = "orjson"
version = "3.11.0"
source = { registry = "https://pypi.org/simple" }
wheels = [{ url = "https://x/orjson-3.11.0-cp39-abi3-manylinux_2_17_x86_64.manylinux2014_x86_64.whl", size = 7 }]

[[package]]
name = "only-source"
version = "1.0"
source = { registry = "https://pypi.org/simple" }
sdist = { url = "https://x/only_source-1.0.tar.gz", size = 5 }
"#;

    #[test]
    fn a_workers_size_counts_the_wheels_its_platform_installs() {
        let lock = Lock::parse(LOCK).unwrap();
        let linux = lock.size(Platform::LinuxX86_64, "3.12");
        // pyarrow cp312 manylinux x86_64 53, six any 11, orjson abi3 7, only-source sdist 5.
        assert_eq!(linux.bytes, 53 + 11 + 7 + 5);
        assert_eq!(linux.built, vec!["only-source"]);
        assert_eq!(lock.size(Platform::MacosArm64, "3.12").bytes, 36 + 11 + 5);
        assert_eq!(lock.size(Platform::LinuxAarch64, "3.12").bytes, 50 + 11 + 5);
    }

    #[test]
    fn workers_combined_count_a_shared_package_once() {
        let a = Lock::parse(LOCK).unwrap();
        let b = Lock::parse(LOCK).unwrap();
        let one = a.size(Platform::LinuxX86_64, "3.12").bytes;
        assert_eq!(
            combined_size(&[a, b], Platform::LinuxX86_64, "3.12").bytes,
            one
        );
    }

    #[test]
    fn the_project_itself_is_not_a_package() {
        let names: Vec<String> = Lock::parse(LOCK)
            .unwrap()
            .packages()
            .into_iter()
            .map(|p| p.name)
            .collect();
        assert_eq!(names, vec!["pyarrow", "six", "orjson", "only-source"]);
    }

    #[test]
    fn a_combined_resolution_lists_the_versions_it_changes() {
        let lock = Lock::parse(LOCK).unwrap();
        let resolved = "# uv pip compile\npyarrow==26.0.0\nsix==1.17.0 ; python_version >= '3'\n    # via feast\nORJSON==3.11.0\n";
        assert_eq!(
            changes(&lock, resolved),
            vec![Change {
                package: "six".into(),
                from: "1.16.0".into(),
                to: "1.17.0".into()
            }]
        );
    }

    #[test]
    fn names_normalize_as_pep_503_says() {
        assert_eq!(normalize("Scikit_Learn"), "scikit-learn");
        assert_eq!(normalize("zope.interface"), "zope-interface");
        assert_eq!(normalize("a--b__c"), "a-b-c");
    }

    #[test]
    fn a_broken_lock_says_so() {
        assert!(matches!(
            Lock::parse("[[package]\n"),
            Err(LockError::Parse(_))
        ));
    }
}
