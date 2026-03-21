use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use overlord_agent_common::AgentLogFileStatus;
use tracing_appender::{
    non_blocking::WorkerGuard,
    rolling::{RollingFileAppender, Rotation},
};
use tracing_subscriber::EnvFilter;

use crate::config::{EmuleAgentConfig, LogRotation};

const LOG_FILE_PREFIX: &str = "overlord-agent-emule.log";

/// Keeps the non-blocking logging worker alive for the lifetime of the process.
pub struct LoggingRuntime {
    _guard: WorkerGuard,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ResolvedLogSettings {
    dir: PathBuf,
    rotation: LogRotation,
    max_files: usize,
}

/// Initializes the agent tracing subscriber so operational logs are written to a rotating file.
pub fn init_file_logging(config: &EmuleAgentConfig) -> Result<LoggingRuntime> {
    let (file_appender, _settings) = build_file_appender(config)?;
    let (writer, guard) = tracing_appender::non_blocking(file_appender);

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new(config.log.level.clone()))
        .with_writer(writer)
        .with_target(false)
        .with_ansi(false)
        .compact()
        .init();

    Ok(LoggingRuntime { _guard: guard })
}

/// Builds the log-file metadata exposed through the agent stats payload.
#[must_use]
pub fn current_log_file_status(config: &EmuleAgentConfig) -> AgentLogFileStatus {
    let settings = resolve_log_settings(config);
    let path = current_log_path_at(&settings, Utc::now());
    let last_write_at = fs::metadata(&path)
        .ok()
        .and_then(|metadata| metadata.modified().ok())
        .map(DateTime::<Utc>::from);

    AgentLogFileStatus {
        path: path.display().to_string(),
        rotation: settings.rotation.label().to_string(),
        max_files: settings.max_files,
        last_write_at,
    }
}

fn build_file_appender(
    config: &EmuleAgentConfig,
) -> Result<(RollingFileAppender, ResolvedLogSettings)> {
    let settings = resolve_log_settings(config);
    fs::create_dir_all(&settings.dir)
        .with_context(|| format!("failed to create log directory {}", settings.dir.display()))?;
    let appender = RollingFileAppender::builder()
        .rotation(settings.rotation.into())
        .filename_prefix(LOG_FILE_PREFIX)
        .max_log_files(settings.max_files)
        .build(&settings.dir)
        .with_context(|| {
            format!(
                "failed to initialize log appender in {}",
                settings.dir.display()
            )
        })?;
    Ok((appender, settings))
}

fn resolve_log_settings(config: &EmuleAgentConfig) -> ResolvedLogSettings {
    let dir = config
        .log
        .dir
        .as_deref()
        .map(PathBuf::from)
        .unwrap_or_else(|| Path::new(&config.agent.state_dir).join("logs"));
    ResolvedLogSettings {
        dir,
        rotation: config.log.rotation,
        max_files: config.log.max_files,
    }
}

fn current_log_path_at(settings: &ResolvedLogSettings, now: DateTime<Utc>) -> PathBuf {
    let filename = match settings.rotation {
        LogRotation::Never => LOG_FILE_PREFIX.to_string(),
        LogRotation::Minutely => format!("{}.{}", LOG_FILE_PREFIX, now.format("%Y-%m-%d-%H-%M")),
        LogRotation::Hourly => format!("{}.{}", LOG_FILE_PREFIX, now.format("%Y-%m-%d-%H")),
        LogRotation::Daily => format!("{}.{}", LOG_FILE_PREFIX, now.format("%Y-%m-%d")),
    };
    settings.dir.join(filename)
}

impl From<LogRotation> for Rotation {
    fn from(value: LogRotation) -> Self {
        match value {
            LogRotation::Minutely => Rotation::MINUTELY,
            LogRotation::Hourly => Rotation::HOURLY,
            LogRotation::Daily => Rotation::DAILY,
            LogRotation::Never => Rotation::NEVER,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{build_file_appender, current_log_path_at, resolve_log_settings};
    use crate::config::{EmuleAgentConfig, LogRotation};
    use std::{fs, io::Write};

    #[test]
    fn resolve_log_settings_defaults_to_state_dir_logs() {
        let mut config = EmuleAgentConfig::default();
        config.agent.state_dir = "c:\\tmp\\p2p-overlord\\logging-defaults".to_string();

        let settings = resolve_log_settings(&config);

        assert_eq!(
            settings.dir.display().to_string(),
            "c:\\tmp\\p2p-overlord\\logging-defaults\\logs"
        );
    }

    #[test]
    fn current_log_path_matches_daily_rotation_pattern() {
        let mut config = EmuleAgentConfig::default();
        config.log.dir = Some("c:\\tmp\\p2p-overlord\\logging-daily".to_string());
        config.log.rotation = LogRotation::Daily;
        let settings = resolve_log_settings(&config);
        let now = chrono::DateTime::parse_from_rfc3339("2026-03-21T12:34:56Z")
            .unwrap()
            .with_timezone(&chrono::Utc);

        let path = current_log_path_at(&settings, now);

        assert_eq!(
            path.display().to_string(),
            "c:\\tmp\\p2p-overlord\\logging-daily\\overlord-agent-emule.log.2026-03-21"
        );
    }

    #[test]
    fn build_file_appender_creates_expected_log_file_on_write() {
        let temp_root = std::env::temp_dir().join(format!(
            "overlord-agent-emule-log-test-{}",
            uuid::Uuid::new_v4()
        ));
        let mut config = EmuleAgentConfig::default();
        config.log.dir = Some(temp_root.display().to_string());
        config.log.rotation = LogRotation::Never;

        let (mut appender, settings) = build_file_appender(&config).unwrap();
        writeln!(appender, "hello from test").unwrap();
        appender.flush().unwrap();

        let log_path = current_log_path_at(&settings, chrono::Utc::now());
        assert!(
            log_path.exists(),
            "expected log file {}",
            log_path.display()
        );

        fs::remove_dir_all(&temp_root).unwrap();
    }
}
