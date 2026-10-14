//! Typed configuration: the tree, the loader, and the startup rules.
//!
//! One source: `RATATOSKR__` environment variables, with `__` between nesting levels, for example
//! `RATATOSKR__DATABASE__URL`. Built-in defaults fill every other key. There is no configuration
//! file. The variable names are an operational contract that the deploy example
//! `deploy/systemd/notes.conf.example` uses.

use std::fmt::Write as _;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::time::Duration;

use figment::Figment;
use figment::providers::Env;
use secrecy::SecretString;
use tracing_subscriber::EnvFilter;

/// The environment prefix.
const ENV_PREFIX: &str = "RATATOSKR__";

/// The domain API listener. Allocated in `ratatoskr-workspace/docs/DEPLOYMENT_TARGET.md`;
/// reached only through Edge.
pub const DEFAULT_API_PORT: u16 = 8099;

/// The operator listener: health, metrics and version. Allocated in
/// `ratatoskr-workspace/docs/DEPLOYMENT_TARGET.md`; host only.
pub const DEFAULT_ADMIN_PORT: u16 = 9471;

/// Everything a Notes process must know before it serves.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NotesConfig {
    /// The operator listener: `/health/live`, `/health/ready`, `/metrics`, `/version`.
    #[serde(default)]
    pub admin: AdminConfig,

    /// The domain API listener that Edge forwards `/v1/notes` to.
    #[serde(default)]
    pub api: ApiConfig,

    /// The `PostgreSQL` connection that holds the `notes` schema. Required.
    pub database: DatabaseConfig,

    /// The custody of the Obsidian secrets of a `sync` connection.
    #[serde(default)]
    pub secrets: SecretsConfig,

    /// The graceful stop.
    #[serde(default)]
    pub shutdown: ShutdownConfig,

    /// Logging.
    #[serde(default)]
    pub telemetry: TelemetryConfig,
}

/// `RATATOSKR__SECRETS__*`.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SecretsConfig {
    /// The file that holds the 32-byte key of the stored Obsidian passwords (workspace design
    /// D4). The service reads it at each `sync` connect, so a host without `sync` connections
    /// needs no key file.
    #[serde(default = "default_key_file")]
    pub key_file: PathBuf,
}

impl Default for SecretsConfig {
    fn default() -> Self {
        Self {
            key_file: default_key_file(),
        }
    }
}

fn default_key_file() -> PathBuf {
    PathBuf::from(crate::api::DEFAULT_KEY_FILE)
}

/// `RATATOSKR__ADMIN__*`.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminConfig {
    /// The operator listener address.
    #[serde(default = "default_admin_bind")]
    pub bind: SocketAddr,
}

impl Default for AdminConfig {
    fn default() -> Self {
        Self {
            bind: default_admin_bind(),
        }
    }
}

/// `RATATOSKR__API__*`.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApiConfig {
    /// The domain API listener address.
    #[serde(default = "default_api_listen_address")]
    pub listen_address: SocketAddr,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            listen_address: default_api_listen_address(),
        }
    }
}

/// `RATATOSKR__DATABASE__*`.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatabaseConfig {
    /// SECRET: the URL carries the password. Renders as `[REDACTED]`.
    pub url: SecretString,
    /// The pool bound. The host has four cores and other tenants; the pool is small on purpose.
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
    /// How long a request waits for a connection before it fails.
    #[serde(default = "default_acquire_timeout_seconds")]
    pub acquire_timeout_seconds: u64,
}

impl DatabaseConfig {
    /// The acquire timeout as a [`Duration`].
    #[must_use]
    pub const fn acquire_timeout(&self) -> Duration {
        Duration::from_secs(self.acquire_timeout_seconds)
    }
}

/// `RATATOSKR__SHUTDOWN__*`.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShutdownConfig {
    /// How long in-flight requests may run after the stop signal.
    #[serde(default = "default_grace_seconds")]
    pub grace_seconds: u64,
}

impl Default for ShutdownConfig {
    fn default() -> Self {
        Self {
            grace_seconds: default_grace_seconds(),
        }
    }
}

/// `RATATOSKR__TELEMETRY__*`.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TelemetryConfig {
    /// `text` for a terminal, `json` for the journal.
    #[serde(default)]
    pub log_format: LogFormat,
    /// A tracing-subscriber `EnvFilter` directive string.
    #[serde(default = "default_log_filter")]
    pub log_filter: String,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            log_format: LogFormat::default(),
            log_filter: default_log_filter(),
        }
    }
}

/// The log line format.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogFormat {
    /// Human-readable lines.
    #[default]
    Text,
    /// One JSON object per line.
    Json,
}

fn default_admin_bind() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), DEFAULT_ADMIN_PORT)
}

fn default_api_listen_address() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), DEFAULT_API_PORT)
}

const fn default_max_connections() -> u32 {
    4
}

const fn default_acquire_timeout_seconds() -> u64 {
    5
}

const fn default_grace_seconds() -> u64 {
    10
}

fn default_log_filter() -> String {
    "info".to_owned()
}

/// One startup-rule violation.
///
/// Every member is `&'static str`, so a supplied value cannot appear in a failure report and the
/// report cannot echo a secret.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Violation {
    /// The dotted configuration path, for example `database.url`.
    pub key: &'static str,
    /// The environment variable that sets it.
    pub env_var: &'static str,
    /// What the rule requires.
    pub rule: &'static str,
}

/// Every reason a Notes process must refuse to start.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ConfigError {
    /// Extraction failed: a wrong type, a missing key, or an unknown key. figment's message is
    /// not interpolated, because it can quote the supplied value.
    #[error("configuration could not be read")]
    Source(#[from] figment::Error),

    /// The configuration parsed but breaks one or more startup rules.
    #[error("configuration is invalid: {} problem(s)", .0.len())]
    Invalid(Vec<Violation>),
}

impl ConfigError {
    /// The operator-facing report, written to stderr before any subscriber exists. It holds no
    /// supplied value.
    #[must_use]
    pub fn report(&self) -> String {
        let mut report = String::from("ratatoskr-notes: refusing to start; ");
        match self {
            Self::Source(error) => {
                // A key name is not a value, so naming it cannot echo a secret.
                let mut path = error.path.clone();
                let kind = match &error.kind {
                    figment::error::Kind::MissingField(field) => {
                        path.push(field.to_string());
                        "a required key is missing"
                    }
                    figment::error::Kind::UnknownField(_, _) => "an unknown key is set",
                    figment::error::Kind::InvalidType(_, _)
                    | figment::error::Kind::InvalidValue(_, _) => {
                        "a value has the wrong type or form"
                    }
                    _ => "a value is not usable",
                };
                let variable = format!("{ENV_PREFIX}{}", path.join("__").to_uppercase());
                let _ = write!(
                    report,
                    "the configuration could not be read: {kind}: {variable}"
                );
            }
            Self::Invalid(violations) => {
                report.push_str("the configuration is invalid:");
                for violation in violations {
                    let _ = write!(
                        report,
                        "\n  {} ({}): {}",
                        violation.key, violation.env_var, violation.rule
                    );
                }
            }
        }
        report
    }

    /// `78`, `EX_CONFIG` from `sysexits.h`. systemd shows it as `EXIT_CONFIG`.
    #[must_use]
    pub const fn exit_code(&self) -> u8 {
        78
    }
}

/// Read the process environment and produce a validated configuration.
///
/// # Errors
///
/// [`ConfigError::Source`] when extraction fails, [`ConfigError::Invalid`] with every violation
/// when a startup rule fails.
#[allow(
    clippy::result_large_err,
    reason = "figment::Error is the payload of ConfigError::Source; the value is built once, at \
              startup, on the path that then exits"
)]
pub fn load() -> Result<NotesConfig, ConfigError> {
    load_from(&Figment::new().merge(Env::prefixed(ENV_PREFIX).split("__")))
}

/// Extract and validate from any figment. The seam for configuration tests.
///
/// # Errors
///
/// As [`load`].
#[allow(
    clippy::result_large_err,
    reason = "as `load`: the error payload is figment::Error by design"
)]
pub fn load_from(figment: &Figment) -> Result<NotesConfig, ConfigError> {
    let config: NotesConfig = figment.extract()?;
    let violations = validate(&config);
    if violations.is_empty() {
        Ok(config)
    } else {
        Err(ConfigError::Invalid(violations))
    }
}

fn validate(config: &NotesConfig) -> Vec<Violation> {
    let mut found = Vec::new();
    if EnvFilter::try_new(&config.telemetry.log_filter).is_err() {
        found.push(Violation {
            key: "telemetry.log_filter",
            env_var: "RATATOSKR__TELEMETRY__LOG_FILTER",
            rule: "must parse as a tracing-subscriber EnvFilter directive string, e.g. info",
        });
    }
    if config.admin.bind == config.api.listen_address {
        found.push(Violation {
            key: "api.listen_address",
            env_var: "RATATOSKR__API__LISTEN_ADDRESS",
            rule: "must differ from the operator listener admin.bind",
        });
    }
    if !(1..=16).contains(&config.database.max_connections) {
        found.push(Violation {
            key: "database.max_connections",
            env_var: "RATATOSKR__DATABASE__MAX_CONNECTIONS",
            rule: "must be 1..=16",
        });
    }
    if !(1..=60).contains(&config.database.acquire_timeout_seconds) {
        found.push(Violation {
            key: "database.acquire_timeout_seconds",
            env_var: "RATATOSKR__DATABASE__ACQUIRE_TIMEOUT_SECONDS",
            rule: "must be 1..=60",
        });
    }
    if !(1..=60).contains(&config.shutdown.grace_seconds) {
        found.push(Violation {
            key: "shutdown.grace_seconds",
            env_var: "RATATOSKR__SHUTDOWN__GRACE_SECONDS",
            rule: "must be 1..=60",
        });
    }
    found
}
