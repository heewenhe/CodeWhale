//! ShannonNet sandbox backend.
//!
//! Routes shell execution to a ShannonNet worker: a signed `cap://sandbox/exec`
//! invocation inside a Task World owned by this installation's durable
//! `codewhale` Agent. The worker may run on another tailnet node; Codewhale
//! addresses the capability name, never a host, and every command leaves a
//! signed receipt readable with `shannon trace`.
//!
//! The protocol is spoken by the `shannon` CLI (`--json`) rather than
//! reimplemented here, so there is exactly one signer and one verifier. This
//! module replaces the `integrations/codewhale/shannon_adapter.rs` sketch in
//! the ShannonNet repository. It owns no model loop: the Engine stays the one
//! turn loop and this backend is one `exec` at a time.
//!
//! Session lifecycle: [`ShannonBackend::new`] runs at backend creation (session
//! start), resolves or creates the `codewhale` Agent, creates a Task World
//! named after the workspace, and attaches the sandbox capability. Each
//! `exec` is one signed invocation in that World.

use std::collections::HashMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde_json::{Value, json};

use super::backend::{SandboxBackend, SandboxKind, SandboxOutput};

/// Name of the durable principal this installation acts as.
pub const AGENT_NAME: &str = "codewhale";
/// Capability invoked for shell execution when the config names none.
pub const DEFAULT_CAPABILITY: &str = "cap://sandbox/exec";
/// The ShannonNet docker worker refuses timeouts above one minute.
const MAX_WORKER_TIMEOUT_MS: u64 = 60_000;

/// A ShannonNet-backed remote execution backend.
#[derive(Debug)]
pub struct ShannonBackend {
    binary: PathBuf,
    home: PathBuf,
    world_id: String,
    capability: String,
    timeout_secs: u64,
}

impl ShannonBackend {
    /// Create the backend and open the session's Task World.
    ///
    /// `binary` is the `shannon` CLI (a bare name resolves on `PATH`), `home`
    /// its state directory, `capability` the `cap://` name to invoke, and
    /// `workspace` names the World. Fails when the CLI is missing or the
    /// World cannot be created; the caller then falls back to local execution
    /// exactly as with any other backend construction error.
    pub fn new(
        binary: PathBuf,
        home: PathBuf,
        capability: String,
        workspace: &Path,
        timeout_secs: u64,
    ) -> Result<Self> {
        let run = |args: &[&str]| run_json_blocking(&binary, &home, args);
        if run(&["agent", "inspect", AGENT_NAME]).is_err() {
            run(&["agent", "create", AGENT_NAME])
                .context("failed to create the codewhale Agent in ShannonNet")?;
        }
        let name = world_name(workspace);
        let objective = format!("Codewhale session in {}", workspace.display());
        let world = run(&[
            "world",
            "create",
            "--for",
            AGENT_NAME,
            "--name",
            &name,
            "--objective",
            &objective,
        ])
        .context("failed to create the session Task World in ShannonNet")?;
        let world_id = world
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .context("ShannonNet world create returned no id")?
            .to_string();
        run(&[
            "world",
            "attach",
            "--world",
            &world_id,
            "--actions",
            "invoke",
            "capability",
            &capability,
        ])
        .with_context(|| format!("failed to attach {capability} to the session World"))?;
        Ok(Self {
            binary,
            home,
            world_id,
            capability,
            timeout_secs,
        })
    }

    /// The Task World this session invokes in (for receipts and inspection).
    #[must_use]
    pub fn world_id(&self) -> &str {
        &self.world_id
    }
}

/// World name derived from the workspace path: stable per project, readable
/// in `shannon trace`.
fn world_name(workspace: &Path) -> String {
    let slug: String = workspace
        .to_string_lossy()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect();
    format!("codewhale:{}", slug.trim_matches('-'))
}

fn cli_args(home: &Path, args: &[&str]) -> Vec<OsString> {
    let mut out = vec![
        OsString::from("--home"),
        home.as_os_str().to_owned(),
        OsString::from("--json"),
    ];
    out.extend(args.iter().map(OsString::from));
    out
}

fn parse_cli_output(
    status: std::process::ExitStatus,
    stdout: &[u8],
    stderr: &[u8],
) -> Result<Value> {
    if !status.success() {
        let stderr = String::from_utf8_lossy(stderr);
        let stderr = stderr.trim();
        bail!(
            "shannon exited with {}: {}",
            status.code().unwrap_or(-1),
            if stderr.is_empty() {
                "(no stderr)"
            } else {
                stderr
            }
        );
    }
    serde_json::from_slice(stdout).context("shannon printed invalid JSON")
}

fn run_json_blocking(binary: &Path, home: &Path, args: &[&str]) -> Result<Value> {
    let output = std::process::Command::new(binary)
        .args(cli_args(home, args))
        .stdin(Stdio::null())
        .output()
        .with_context(|| format!("failed to run {}", binary.display()))?;
    parse_cli_output(output.status, &output.stdout, &output.stderr)
}

async fn run_json(binary: &Path, home: &Path, args: &[&str]) -> Result<Value> {
    let output = tokio::process::Command::new(binary)
        .args(cli_args(home, args))
        .stdin(Stdio::null())
        .output()
        .await
        .with_context(|| format!("failed to run {}", binary.display()))?;
    parse_cli_output(output.status, &output.stdout, &output.stderr)
}

/// Map a worker's signed output to the backend contract. The docker worker
/// kind reports `stdout`, `stderr`, and `exit_code`; a worker kind that only
/// reports `output`/`ok` is mapped conservatively.
fn sandbox_output_from(result: &Value) -> Result<SandboxOutput> {
    let response = result
        .get("response")
        .context("invoke result has no response")?;
    if let Some(err) = response.get("error").and_then(Value::as_str)
        && !err.is_empty()
    {
        bail!("ShannonNet provider error: {err}");
    }
    let output = response
        .get("output")
        .context("invoke response has no output")?;
    let field = |name: &str| {
        output
            .get(name)
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string()
    };
    let exit_code = match output.get("exit_code").and_then(Value::as_i64) {
        Some(code) => i32::try_from(code).unwrap_or(1),
        None if output.get("ok").and_then(Value::as_bool) == Some(false) => 1,
        None => 0,
    };
    let stdout = if output.get("stdout").is_some() {
        field("stdout")
    } else {
        field("output")
    };
    Ok(SandboxOutput {
        stdout,
        stderr: field("stderr"),
        exit_code,
    })
}

#[async_trait]
impl SandboxBackend for ShannonBackend {
    fn kind(&self) -> SandboxKind {
        SandboxKind::Shannon
    }

    async fn exec(&self, cmd: &str, env: &HashMap<String, String>) -> Result<SandboxOutput> {
        let timeout_ms = (self.timeout_secs * 1000).min(MAX_WORKER_TIMEOUT_MS);
        let input = json!({"command": cmd, "env": env, "timeout_ms": timeout_ms}).to_string();
        let result = run_json(
            &self.binary,
            &self.home,
            &[
                "cap",
                "invoke",
                "--agent",
                AGENT_NAME,
                "--world",
                &self.world_id,
                "--cap",
                &self.capability,
                "--input",
                &input,
            ],
        )
        .await
        .context("ShannonNet invocation failed")?;
        sandbox_output_from(&result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn world_name_is_stable_and_readable() {
        assert_eq!(
            world_name(Path::new("/Volumes/VIX/CW/ShannonNet")),
            "codewhale:Volumes-VIX-CW-ShannonNet"
        );
    }

    #[test]
    fn maps_docker_kind_output() {
        let result = json!({"response": {"output": {"stdout": "hi\n", "stderr": "warn", "exit_code": 3, "ok": false}}});
        let out = sandbox_output_from(&result).unwrap();
        assert_eq!(
            (out.stdout.as_str(), out.stderr.as_str(), out.exit_code),
            ("hi\n", "warn", 3)
        );
    }

    #[test]
    fn maps_legacy_output_and_provider_error() {
        let legacy = json!({"response": {"output": {"output": "x", "ok": false}}});
        let out = sandbox_output_from(&legacy).unwrap();
        assert_eq!((out.stdout.as_str(), out.exit_code), ("x", 1));
        let failed = json!({"response": {"error": "container execution failed: timed out"}});
        let err = sandbox_output_from(&failed).unwrap_err().to_string();
        assert!(err.contains("timed out"), "{err}");
    }

    /// A stand-in `shannon` CLI: records every argv line and answers with the
    /// JSON the real CLI prints for each subcommand.
    #[cfg(unix)]
    fn fake_shannon(dir: &Path, agent_exists: bool) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let log = dir.join("argv.log");
        let inspect_exit = if agent_exists { 0 } else { 1 };
        let script = format!(
            r#"#!/bin/sh
printf '%s\n' "$*" >> "{log}"
shift 3 # --home DIR --json
case "$1 $2" in
  "agent inspect") [ {inspect_exit} -eq 0 ] && printf '{{"id":"agent-1","name":"codewhale"}}'; exit {inspect_exit} ;;
  "agent create") printf '{{"id":"agent-1","name":"codewhale"}}' ;;
  "world create") printf '{{"id":"world-1","name":"%s"}}' "$6" ;;
  "world attach") printf '{{"capability":"%s"}}' "$8" ;;
  "cap invoke")
    # echo the command back through the docker-kind result shape
    cmd=$(printf '%s' "${{10}}" | sed -n 's/.*"command":"\([^"]*\)".*/\1/p')
    if [ "$cmd" = "false" ]; then
      printf '{{"response":{{"output":{{"stdout":"","stderr":"boom","exit_code":1,"ok":false}}}},"route":{{"selected_provider_id":"prov-1"}}}}'
    else
      printf '{{"response":{{"output":{{"stdout":"ran: %s","stderr":"","exit_code":0,"ok":true}}}},"route":{{"selected_provider_id":"prov-1"}}}}' "$cmd"
    fi ;;
  *) echo "unexpected: $*" >&2; exit 2 ;;
esac
"#,
            log = log.display()
        );
        let path = dir.join("shannon");
        std::fs::write(&path, script).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        path
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn session_start_creates_world_then_exec_invokes_in_it() {
        let dir = tempfile::tempdir().unwrap();
        let binary = fake_shannon(dir.path(), false);
        let home = dir.path().join("home");
        let backend = ShannonBackend::new(
            binary,
            home.clone(),
            DEFAULT_CAPABILITY.to_string(),
            Path::new("/tmp/proj"),
            30,
        )
        .unwrap();
        assert_eq!(backend.world_id(), "world-1");

        let out = backend.exec("echo hi", &HashMap::new()).await.unwrap();
        assert_eq!((out.stdout.as_str(), out.exit_code), ("ran: echo hi", 0));
        let failed = backend.exec("false", &HashMap::new()).await.unwrap();
        assert_eq!((failed.stderr.as_str(), failed.exit_code), ("boom", 1));

        let log = std::fs::read_to_string(dir.path().join("argv.log")).unwrap();
        let lines: Vec<&str> = log.lines().collect();
        let home_flag = format!("--home {} --json", home.display());
        assert!(lines.iter().all(|l| l.starts_with(&home_flag)), "{log}");
        assert!(lines[0].contains("agent inspect codewhale"), "{log}");
        assert!(lines[1].contains("agent create codewhale"), "{log}");
        assert!(
            lines[2].contains("world create --for codewhale --name codewhale:tmp-proj"),
            "{log}"
        );
        assert!(
            lines[3].contains(
                "world attach --world world-1 --actions invoke capability cap://sandbox/exec"
            ),
            "{log}"
        );
        assert!(
            lines[4].contains(
                "cap invoke --agent codewhale --world world-1 --cap cap://sandbox/exec --input"
            ),
            "{log}"
        );
        assert!(lines[4].contains(r#""timeout_ms":30000"#), "{log}");
        assert_eq!(lines.len(), 6);
    }

    #[cfg(unix)]
    #[test]
    fn existing_agent_is_not_recreated() {
        let dir = tempfile::tempdir().unwrap();
        let binary = fake_shannon(dir.path(), true);
        ShannonBackend::new(
            binary,
            dir.path().join("home"),
            "cap://x".into(),
            Path::new("/p"),
            5,
        )
        .unwrap();
        let log = std::fs::read_to_string(dir.path().join("argv.log")).unwrap();
        assert!(!log.contains("agent create"), "{log}");
        assert!(log.contains("capability cap://x"), "{log}");
    }

    #[test]
    fn missing_binary_is_a_construction_error() {
        let err = ShannonBackend::new(
            PathBuf::from("/nonexistent/shannon-cli"),
            PathBuf::from("/tmp"),
            "cap://x".into(),
            Path::new("/p"),
            5,
        )
        .unwrap_err();
        assert!(err.to_string().contains("codewhale Agent"), "{err}");
    }
}
