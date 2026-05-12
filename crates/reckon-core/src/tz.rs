//! Time-zone resolution for the `--tz` CLI flag.
//!
//! Accepts `local` (system zone via `iana_time_zone`), `utc`, or any IANA
//! identifier (e.g. `America/New_York`). The default for callers that don't
//! supply a value is `local` — see `crates/reckon-cli/src/main.rs`.

use std::fmt;

/// Errors that can arise from `resolve_tz`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TzResolveError {
    /// The system time zone could not be detected when resolving `local`.
    LocalUnavailable(String),
    /// The supplied IANA name was not found in the tzdata database.
    UnknownZone(String),
}

impl fmt::Display for TzResolveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LocalUnavailable(reason) => {
                write!(f, "failed to detect local time zone: {reason}")
            }
            Self::UnknownZone(name) => {
                write!(f, "unknown time zone: {name}")
            }
        }
    }
}

impl std::error::Error for TzResolveError {}

/// Resolve a CLI `--tz` value to a concrete jiff `TimeZone`.
///
/// - `"local"` (case-insensitive) — read `/etc/localtime` via `iana_time_zone`.
/// - `"utc"` (case-insensitive) — `TimeZone::UTC`.
/// - Anything else — looked up as an IANA name via `TimeZone::get`.
///
/// # Errors
///
/// Returns `TzResolveError::LocalUnavailable` if `name` is "local" but the
/// system zone cannot be detected, and `TzResolveError::UnknownZone` if the
/// supplied identifier is not in the tzdata database.
pub fn resolve_tz(name: &str) -> Result<jiff::tz::TimeZone, TzResolveError> {
    let trimmed = name.trim();
    if trimmed.eq_ignore_ascii_case("local") {
        let local = iana_time_zone::get_timezone()
            .map_err(|error| TzResolveError::LocalUnavailable(error.to_string()))?;
        return jiff::tz::TimeZone::get(&local)
            .map_err(|_| TzResolveError::UnknownZone(local));
    }
    if trimmed.eq_ignore_ascii_case("utc") {
        return Ok(jiff::tz::TimeZone::UTC);
    }
    jiff::tz::TimeZone::get(trimmed).map_err(|_| TzResolveError::UnknownZone(trimmed.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_utc_case_insensitive() {
        assert!(resolve_tz("utc").is_ok());
        assert!(resolve_tz("UTC").is_ok());
        assert!(resolve_tz(" Utc ").is_ok());
    }

    #[test]
    fn resolves_iana_name() {
        assert!(resolve_tz("America/New_York").is_ok());
        assert!(resolve_tz("America/Los_Angeles").is_ok());
    }

    #[test]
    fn unknown_zone_errors() {
        let err = resolve_tz("Not/A_Real_Zone").expect_err("should be unknown zone");
        match err {
            TzResolveError::UnknownZone(name) => assert_eq!(name, "Not/A_Real_Zone"),
            TzResolveError::LocalUnavailable(reason) => {
                panic!("expected UnknownZone, got LocalUnavailable({reason})")
            }
        }
    }

    #[test]
    fn resolves_local() {
        // Don't assert the exact zone — CI/dev machines vary — just that it
        // resolves without error or surfaces a LocalUnavailable.
        match resolve_tz("local") {
            Ok(_) | Err(TzResolveError::LocalUnavailable(_)) => {}
            Err(other) => panic!("unexpected error resolving local: {other}"),
        }
    }
}
