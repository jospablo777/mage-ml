use std::fmt;

/// Invalid requests are the caller's to fix; engine errors come from reading or querying
/// the file. Messages never carry the file's path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Invalid(String),
    Engine(String),
}

impl Error {
    pub fn invalid(message: impl Into<String>) -> Self {
        Error::Invalid(message.into())
    }

    pub fn engine(message: impl Into<String>) -> Self {
        Error::Engine(message.into())
    }

    pub fn message(&self) -> &str {
        match self {
            Error::Invalid(message) | Error::Engine(message) => message,
        }
    }

    /// The error with every occurrence of the path replaced, for messages that reach users.
    pub fn without_path(self, path: &str) -> Self {
        if path.is_empty() {
            return self;
        }
        match self {
            Error::Invalid(message) => Error::Invalid(message.replace(path, "<source>")),
            Error::Engine(message) => Error::Engine(message.replace(path, "<source>")),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message())
    }
}

impl std::error::Error for Error {}

impl From<polars::prelude::PolarsError> for Error {
    fn from(error: polars::prelude::PolarsError) -> Self {
        Error::Engine(error.to_string())
    }
}

const PANIC_DETAIL_CHARS: usize = 300;

/// Runs `query` and turns a panic in it, such as Polars meeting a type it cannot read,
/// into an engine error. A panic used to end the query process.
pub fn guarded<T>(path: &str, query: impl FnOnce() -> Result<T, Error>) -> Result<T, Error> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(query)) {
        Ok(result) => result,
        Err(payload) => {
            let detail = payload
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| payload.downcast_ref::<&str>().copied())
                .unwrap_or("unknown failure");
            let mut detail: String = detail.chars().take(PANIC_DETAIL_CHARS).collect();
            if detail.chars().count() == PANIC_DETAIL_CHARS {
                detail.push('…');
            }
            Err(Error::Engine(format!("The query engine failed: {detail}")).without_path(path))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_panic_becomes_an_engine_error_without_the_path() {
        let result: Result<(), Error> = guarded("/data/secret.parquet", || {
            panic!("bad type in /data/secret.parquet")
        });
        assert_eq!(
            result,
            Err(Error::Engine(
                "The query engine failed: bad type in <source>".to_string()
            ))
        );
        let long: Result<(), Error> = guarded("", || panic!("{}", "x".repeat(1000)));
        assert!(long.unwrap_err().message().chars().count() < 340);
        assert_eq!(guarded("", || Ok(7)), Ok(7));
    }
}
