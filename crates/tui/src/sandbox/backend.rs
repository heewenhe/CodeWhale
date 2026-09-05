//! Pluggable sandbox backend abstraction.
//!
//! External sandbox backends route shell command execution to a remote service
//! (Alibaba OpenSandbox, or a ShannonNet worker reached by capability name)
//! instead of spawning a local process. This is complementary to the OS-level
//! sandbox module (Seatbelt / opt-in bubblewrap) — the external backend
//! *replaces* local execution entirely when configured.

use std::collections::HashMap;
use std::path::Path;

use anyhow::Result;
use async_trait::async_trait;

/// Output from a sandbox backend execution.
#[derive(Debug, Clone)]
pub struct SandboxOutput {
    /// Standard output from the command.
    pub stdout: String,
    /// Standard error from the command.
    pub stderr: String,
    /// Exit code (0 for success).
    pub exit_code: i32,
}

/// The kind of external sandbox backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxKind {
    /// No external sandbox — execute commands locally.
    None,
    /// Alibaba OpenSandbox remote execution.
    OpenSandbox,
    /// ShannonNet: a signed capability invocation on a worker that may live
    /// on another tailnet node (see `shannon.rs`).
    Shannon,
}

impl SandboxKind {
    /// Parse a sandbox backend name from config (case-insensitive).
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "none" | "" => Some(Self::None),
            "opensandbox" | "open-sandbox" | "open_sandbox" => Some(Self::OpenSandbox),
            "shannon" | "shannonnet" | "shannon-net" => Some(Self::Shannon),
            _ => None,
        }
    }

    /// Human-readable label.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::OpenSandbox => "opensandbox",
            Self::Shannon => "shannon",
        }
    }
}

/// Abstract interface for an external sandbox backend.
///
/// Implementations send commands to a remote execution environment and return
/// structured output. The trait is `Send + Sync` so it can be stored in an
/// `Arc` and shared across async tasks.
#[async_trait]
pub trait SandboxBackend: Send + Sync {
    /// Which backend this is, for tool metadata and receipts.
    fn kind(&self) -> SandboxKind;

    /// Execute a shell command and return its output.
    ///
    /// `cmd` is the full shell command string (e.g. `"ls -la"`).
    /// `env` contains additional environment variables to set.
    async fn exec(&self, cmd: &str, env: &HashMap<String, String>) -> Result<SandboxOutput>;
}

use crate::config::Config;

/// Create the configured sandbox backend from config.
///
/// Returns `None` when no external sandbox backend is configured (i.e. the
/// `sandbox_backend` key is absent, empty, or `"none"`). When `"opensandbox"`
/// is set, constructs an [`OpenSandboxBackend`](super::opensandbox::OpenSandboxBackend) using `sandbox_url` and
/// `sandbox_api_key`. When `"shannon"` is set, constructs a
/// [`ShannonBackend`](super::shannon::ShannonBackend), which opens a Task
/// World for `workspace` at construction (session start).
pub fn create_backend(
    config: &Config,
    workspace: &Path,
) -> Result<Option<Box<dyn SandboxBackend>>> {
    let kind = config
        .sandbox_backend
        .as_deref()
        .and_then(SandboxKind::parse)
        .unwrap_or(SandboxKind::None);

    match kind {
        SandboxKind::None => Ok(None),
        SandboxKind::OpenSandbox => {
            let base_url = config
                .sandbox_url
                .clone()
                .unwrap_or_else(|| "http://localhost:8080".to_string());
            let api_key = config.sandbox_api_key.clone();
            let backend = super::opensandbox::OpenSandboxBackend::new(base_url, api_key, 30)?;
            Ok(Some(Box::new(backend)))
        }
        SandboxKind::Shannon => {
            let binary = std::env::var_os("SHANNON")
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|| std::path::PathBuf::from("shannon"));
            let home = config
                .sandbox_shannon_home
                .as_deref()
                .map(crate::config::expand_path)
                .or_else(|| std::env::var_os("SHANNON_HOME").map(std::path::PathBuf::from))
                .unwrap_or_else(|| {
                    dirs::home_dir()
                        .unwrap_or_else(|| std::path::PathBuf::from("."))
                        .join(".shannon")
                });
            let capability = config
                .sandbox_shannon_capability
                .clone()
                .filter(|cap| !cap.trim().is_empty())
                .unwrap_or_else(|| super::shannon::DEFAULT_CAPABILITY.to_string());
            let sync = config.sandbox_shannon_sync.unwrap_or(true);
            let backend =
                super::shannon::ShannonBackend::new(binary, home, capability, workspace, 30, sync)?;
            Ok(Some(Box::new(backend)))
        }
    }
}
