use std::{env, path::PathBuf};

const OVERLORD_TMP_DIR_ENV: &str = "OVERLORD_TMP_DIR";
const OVERLORD_LOG_DIR_ENV: &str = "OVERLORD_LOG_DIR";
const DEFAULT_WORKSPACE_TMP_DIR_NAME: &str = "p2p-overlord";

fn read_env_path(name: &str) -> Option<PathBuf> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

#[cfg(test)]
fn read_env_path_with<F>(name: &str, read_env: F) -> Option<PathBuf>
where
    F: Fn(&str) -> Option<String>,
{
    read_env(name)
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

#[cfg(test)]
fn workspace_tmp_dir_from<F>(read_env: F) -> PathBuf
where
    F: Fn(&str) -> Option<String>,
{
    read_env_path_with(OVERLORD_TMP_DIR_ENV, read_env)
        .unwrap_or_else(|| env::temp_dir().join(DEFAULT_WORKSPACE_TMP_DIR_NAME))
}

#[cfg(test)]
fn workspace_log_dir_from<F>(read_env: F) -> PathBuf
where
    F: Fn(&str) -> Option<String> + Copy,
{
    read_env_path_with(OVERLORD_LOG_DIR_ENV, read_env)
        .unwrap_or_else(|| workspace_tmp_dir_from(read_env))
}

/// Resolves the shared workspace temp directory for the agent runtime and tests.
#[must_use]
pub(crate) fn workspace_tmp_dir() -> PathBuf {
    read_env_path(OVERLORD_TMP_DIR_ENV)
        .unwrap_or_else(|| env::temp_dir().join(DEFAULT_WORKSPACE_TMP_DIR_NAME))
}

/// Resolves the shared workspace log directory for the agent runtime and tests.
#[must_use]
pub(crate) fn workspace_log_dir() -> PathBuf {
    read_env_path(OVERLORD_LOG_DIR_ENV).unwrap_or_else(workspace_tmp_dir)
}

/// Builds a unique test directory under the shared workspace temp root.
#[cfg(test)]
#[must_use]
pub(crate) fn unique_test_dir(prefix: &str) -> PathBuf {
    workspace_tmp_dir().join(format!("{prefix}-{}", uuid::Uuid::new_v4()))
}

#[cfg(test)]
mod tests {
    use super::{read_env_path_with, workspace_log_dir_from, workspace_tmp_dir_from};
    use std::path::PathBuf;

    #[test]
    fn read_env_path_ignores_blank_values() {
        let resolved = read_env_path_with("OVERLORD_TMP_DIR", |_| Some("   ".to_string()));
        assert_eq!(resolved, None);
    }

    #[test]
    fn workspace_tmp_dir_prefers_env_override() {
        let resolved = workspace_tmp_dir_from(|name| match name {
            "OVERLORD_TMP_DIR" => Some("c:\\tmp\\custom-workspace".to_string()),
            _ => None,
        });

        assert_eq!(resolved, PathBuf::from("c:\\tmp\\custom-workspace"));
    }

    #[test]
    fn workspace_log_dir_prefers_explicit_log_override() {
        let resolved = workspace_log_dir_from(|name| match name {
            "OVERLORD_LOG_DIR" => Some("c:\\tmp\\custom-logs".to_string()),
            "OVERLORD_TMP_DIR" => Some("c:\\tmp\\custom-tmp".to_string()),
            _ => None,
        });

        assert_eq!(resolved, PathBuf::from("c:\\tmp\\custom-logs"));
    }

    #[test]
    fn workspace_log_dir_falls_back_to_workspace_tmp_dir() {
        let resolved = workspace_log_dir_from(|name| match name {
            "OVERLORD_TMP_DIR" => Some("c:\\tmp\\custom-tmp".to_string()),
            _ => None,
        });

        assert_eq!(resolved, PathBuf::from("c:\\tmp\\custom-tmp"));
    }
}
