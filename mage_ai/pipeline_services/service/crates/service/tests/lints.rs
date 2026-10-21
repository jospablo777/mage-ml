//! The error-message rule (QUALITY.md): every error enum in the workspace has a message for
//! each variant, through thiserror or a Display impl, and none of them is empty. Panics in
//! the HTTP handlers are denied by clippy in api.rs.

use std::fs;
use std::path::{Path, PathBuf};

fn sources(folder: &Path, found: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(folder).expect("readable folder") {
        let path = entry.expect("entry").path();
        if path.is_dir() {
            if path.file_name().is_some_and(|name| name != "target") {
                sources(&path, found);
            }
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            found.push(path);
        }
    }
}

/// Crate folder to the text of its src files.
fn crates() -> Vec<(PathBuf, String)> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    let mut crates = Vec::new();
    for entry in fs::read_dir(&root).expect("crates folder") {
        let folder = entry.expect("entry").path().join("src");
        if !folder.is_dir() {
            continue;
        }
        let mut files = Vec::new();
        sources(&folder, &mut files);
        let text = files
            .iter()
            .map(|file| fs::read_to_string(file).expect("readable source"))
            .collect::<Vec<_>>()
            .join("\n");
        crates.push((folder, text));
    }
    crates
}

/// The error enums of a crate's source with whether each has messages.
fn error_enums(text: &str) -> Vec<(String, bool)> {
    let lines: Vec<&str> = text.lines().collect();
    let mut found = Vec::new();
    for (index, line) in lines.iter().enumerate() {
        let declaration = line.trim_start().trim_start_matches("pub(crate) ");
        let declaration = declaration.trim_start_matches("pub ");
        let Some(rest) = declaration.strip_prefix("enum ") else {
            continue;
        };
        let name: String = rest
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        if !name.ends_with("Error") {
            continue;
        }
        let derives = lines[index.saturating_sub(4)..index]
            .iter()
            .any(|line| line.contains("derive(") && line.contains("Error"));
        let display = text.contains(&format!("Display for {name} "))
            || text.contains(&format!("Display for {name}<"));
        found.push((name, derives || display));
    }
    found
}

#[test]
fn every_error_enum_has_messages() {
    let mut problems = Vec::new();
    let mut counted = 0;
    for (folder, text) in crates() {
        for (name, has_messages) in error_enums(&text) {
            counted += 1;
            if !has_messages {
                problems.push(format!(
                    "{name} in {}: derive thiserror::Error or implement Display",
                    folder.display()
                ));
            }
        }
        if text.contains("#[error(\"\")]") {
            problems.push(format!("an empty #[error(\"\")] in {}", folder.display()));
        }
    }
    assert!(counted >= 6, "the error enums were found ({counted})");
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

#[test]
fn the_rule_finds_an_error_without_messages() {
    let text = "#[derive(Debug)]\npub enum LoadError {\n    Missing,\n}\n";
    assert_eq!(error_enums(text), vec![("LoadError".to_string(), false)]);
    let with_display = format!("{text}impl std::fmt::Display for LoadError {{}}\n");
    assert_eq!(
        error_enums(&with_display),
        vec![("LoadError".to_string(), true)]
    );
    let derived =
        "#[derive(Debug, thiserror::Error)]\nenum ReadError {\n    #[error(\"x\")]\n    X,\n}\n";
    assert_eq!(error_enums(derived), vec![("ReadError".to_string(), true)]);
}
