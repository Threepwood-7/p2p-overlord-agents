use std::{
    fs::{self, File},
    io::{self, Write},
    path::PathBuf,
};

use anyhow::{Context, Result};
use chrono::{DateTime, Datelike, Timelike, Utc};
use overlord_agent_common::AgentLogFileStatus;
use tracing_appender::{
    non_blocking::WorkerGuard,
    rolling::{RollingFileAppender, Rotation},
};
use tracing_subscriber::EnvFilter;

use crate::config::{EmuleAgentConfig, LogRotation};
use crate::paths::workspace_log_dir;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HeadPeriod {
    Never,
    Minutely {
        year: i32,
        month: u32,
        day: u32,
        hour: u32,
        minute: u32,
    },
    Hourly {
        year: i32,
        month: u32,
        day: u32,
        hour: u32,
    },
    Daily {
        year: i32,
        month: u32,
        day: u32,
    },
}

/// Initializes the agent tracing subscriber so operational logs are written to a rotating file.
pub fn init_file_logging(config: &EmuleAgentConfig) -> Result<LoggingRuntime> {
    let (file_writer, _settings) = build_file_writer(config)?;
    let (writer, guard) = tracing_appender::non_blocking(file_writer);

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
    let path = current_log_path(&settings);
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

fn build_file_writer(config: &EmuleAgentConfig) -> Result<(DualFileWriter, ResolvedLogSettings)> {
    let settings = resolve_log_settings(config);
    fs::create_dir_all(&settings.dir)
        .with_context(|| format!("failed to create log directory {}", settings.dir.display()))?;
    let writer = DualFileWriter::new(&settings).with_context(|| {
        format!(
            "failed to initialize log writer in {}",
            settings.dir.display()
        )
    })?;
    Ok((writer, settings))
}

fn resolve_log_settings(config: &EmuleAgentConfig) -> ResolvedLogSettings {
    let dir = config
        .log
        .dir
        .as_deref()
        .map(PathBuf::from)
        .unwrap_or_else(workspace_log_dir);
    ResolvedLogSettings {
        dir,
        rotation: config.log.rotation,
        max_files: config.log.max_files,
    }
}

fn current_log_path(settings: &ResolvedLogSettings) -> PathBuf {
    settings.dir.join(LOG_FILE_PREFIX)
}

#[cfg(test)]
fn current_rotated_log_path_at(settings: &ResolvedLogSettings, now: DateTime<Utc>) -> PathBuf {
    let filename = match settings.rotation {
        LogRotation::Never => LOG_FILE_PREFIX.to_string(),
        LogRotation::Minutely => format!("{}.{}", LOG_FILE_PREFIX, now.format("%Y-%m-%d-%H-%M")),
        LogRotation::Hourly => format!("{}.{}", LOG_FILE_PREFIX, now.format("%Y-%m-%d-%H")),
        LogRotation::Daily => format!("{}.{}", LOG_FILE_PREFIX, now.format("%Y-%m-%d")),
    };
    settings.dir.join(filename)
}

fn period_for(rotation: LogRotation, now: DateTime<Utc>) -> HeadPeriod {
    match rotation {
        LogRotation::Never => HeadPeriod::Never,
        LogRotation::Minutely => HeadPeriod::Minutely {
            year: now.year(),
            month: now.month(),
            day: now.day(),
            hour: now.hour(),
            minute: now.minute(),
        },
        LogRotation::Hourly => HeadPeriod::Hourly {
            year: now.year(),
            month: now.month(),
            day: now.day(),
            hour: now.hour(),
        },
        LogRotation::Daily => HeadPeriod::Daily {
            year: now.year(),
            month: now.month(),
            day: now.day(),
        },
    }
}

/// Mirrors tracing-appender's rotating output into a stable `*.log` head file.
struct DualFileWriter {
    settings: ResolvedLogSettings,
    rotating: RollingFileAppender,
    head_file: File,
    head_period: HeadPeriod,
}

impl DualFileWriter {
    fn new(settings: &ResolvedLogSettings) -> Result<Self> {
        let rotating = RollingFileAppender::builder()
            .rotation(settings.rotation.into())
            .filename_prefix(LOG_FILE_PREFIX)
            .max_log_files(settings.max_files)
            .build(&settings.dir)
            .with_context(|| {
                format!(
                    "failed to initialize rotating log appender in {}",
                    settings.dir.display()
                )
            })?;
        let now = Utc::now();
        let head_file = open_head_file(current_log_path(settings), true)?;
        Ok(Self {
            settings: settings.clone(),
            rotating,
            head_file,
            head_period: period_for(settings.rotation, now),
        })
    }

    fn refresh_head_file_if_needed(&mut self) -> io::Result<()> {
        let next_period = period_for(self.settings.rotation, Utc::now());
        if next_period == self.head_period {
            return Ok(());
        }

        self.head_file.flush()?;
        self.head_file = open_head_file(current_log_path(&self.settings), true)?;
        self.head_period = next_period;
        Ok(())
    }
}

fn open_head_file(path: PathBuf, truncate: bool) -> io::Result<File> {
    let mut options = fs::OpenOptions::new();
    options.create(true).write(true);
    if truncate {
        options.truncate(true);
    } else {
        options.append(true);
    }
    options.open(path)
}

impl Write for DualFileWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.refresh_head_file_if_needed()?;
        self.rotating.write_all(buf)?;
        self.head_file.write_all(buf)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.rotating.flush()?;
        self.head_file.flush()
    }
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
    use super::{
        build_file_writer, current_log_path, current_rotated_log_path_at, period_for,
        resolve_log_settings,
    };
    use crate::config::{EmuleAgentConfig, LogRotation};
    use crate::paths::{unique_test_dir, workspace_log_dir};
    use std::{fs, io::Write};

    #[test]
    fn resolve_log_settings_defaults_to_workspace_log_dir() {
        let config = EmuleAgentConfig::default();

        let settings = resolve_log_settings(&config);

        assert_eq!(settings.dir, workspace_log_dir());
    }

    #[test]
    fn current_log_path_uses_stable_head_filename() {
        let mut config = EmuleAgentConfig::default();
        config.log.dir = Some("c:\\tmp\\logs\\logging-daily".to_string());
        config.log.rotation = LogRotation::Daily;
        let settings = resolve_log_settings(&config);

        assert_eq!(
            current_log_path(&settings).display().to_string(),
            "c:\\tmp\\logs\\logging-daily\\overlord-agent-emule.log"
        );
    }

    #[test]
    fn current_rotated_log_path_matches_daily_rotation_pattern() {
        let mut config = EmuleAgentConfig::default();
        config.log.dir = Some("c:\\tmp\\logs\\logging-daily".to_string());
        config.log.rotation = LogRotation::Daily;
        let settings = resolve_log_settings(&config);
        let now = chrono::DateTime::parse_from_rfc3339("2026-03-21T12:34:56Z")
            .unwrap()
            .with_timezone(&chrono::Utc);

        let path = current_rotated_log_path_at(&settings, now);

        assert_eq!(
            path.display().to_string(),
            "c:\\tmp\\logs\\logging-daily\\overlord-agent-emule.log.2026-03-21"
        );
    }

    #[test]
    fn build_file_writer_creates_head_and_rotated_logs_on_write() {
        let temp_root = unique_test_dir("overlord-agent-emule-log-test");
        let mut config = EmuleAgentConfig::default();
        config.log.dir = Some(temp_root.display().to_string());
        config.log.rotation = LogRotation::Daily;

        let (mut writer, settings) = build_file_writer(&config).unwrap();
        writeln!(writer, "hello from test").unwrap();
        writer.flush().unwrap();

        let head_path = current_log_path(&settings);
        let rotated_path = current_rotated_log_path_at(&settings, chrono::Utc::now());
        assert!(
            head_path.exists(),
            "expected head log file {}",
            head_path.display()
        );
        assert!(
            rotated_path.exists(),
            "expected rotated log file {}",
            rotated_path.display()
        );

        fs::remove_dir_all(&temp_root).unwrap();
    }

    #[test]
    fn period_for_daily_changes_when_date_changes() {
        let left = chrono::DateTime::parse_from_rfc3339("2026-03-21T23:59:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let right = chrono::DateTime::parse_from_rfc3339("2026-03-22T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);

        assert_ne!(
            period_for(LogRotation::Daily, left),
            period_for(LogRotation::Daily, right)
        );
    }
}
