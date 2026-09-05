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
//!
//! Workspace sync: with `sync` on (the default), the backend ships the
//! session's working tree into the worker's per-World session container
//! before each command — a full archive first, then only what changed
//! (added/modified files, deletions) — so remote builds and tests see the
//! files the Engine just edited locally, not the worker's own checkout.
//! Ignored files (`.gitignore`, `.git`) never leave the machine. The worker
//! enforces path safety and size budgets on its side.

use std::collections::HashMap;
use std::ffi::OsString;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::SystemTime;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use base64::Engine as _;
use serde_json::{Value, json};

use super::backend::{SandboxBackend, SandboxKind, SandboxOutput};

/// Name of the durable principal this installation acts as.
pub const AGENT_NAME: &str = "codewhale";
/// Capability invoked for shell execution when the config names none.
pub const DEFAULT_CAPABILITY: &str = "cap://sandbox/exec";
/// The ShannonNet docker worker refuses command timeouts above 15 minutes.
const MAX_WORKER_TIMEOUT_MS: u64 = 15 * 60 * 1000;
/// Raw bytes per sync archive; the worker reads at most 16 MiB per request
/// and base64 inflates by a third.
const SYNC_CHUNK_BYTES: u64 = 6 << 20;
/// Files above this size are not synced (a receipt names them).
const SYNC_MAX_FILE_BYTES: u64 = 16 << 20;

/// A ShannonNet-backed remote execution backend.
#[derive(Debug)]
pub struct ShannonBackend {
    cli: Cli,
    world_id: String,
    capability: String,
    timeout_secs: u64,
    sync: Option<tokio::sync::Mutex<WorkspaceSync>>,
}

/// The `shannon` CLI and its state directory.
#[derive(Debug, Clone)]
struct Cli {
    binary: PathBuf,
    home: PathBuf,
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
        sync: bool,
    ) -> Result<Self> {
        let cli = Cli { binary, home };
        let run = |args: &[&str]| run_json_blocking(&cli.binary, &cli.home, args);
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
        // Sync needs the worker's session actions; without sync the grant
        // stays as narrow as before.
        let actions = if sync {
            "invoke,sync,destroy"
        } else {
            "invoke"
        };
        run(&[
            "world",
            "attach",
            "--world",
            &world_id,
            "--actions",
            actions,
            "capability",
            &capability,
        ])
        .with_context(|| format!("failed to attach {capability} to the session World"))?;
        Ok(Self {
            cli,
            world_id,
            capability,
            timeout_secs,
            sync: sync
                .then(|| tokio::sync::Mutex::new(WorkspaceSync::new(workspace.to_path_buf()))),
        })
    }

    fn invoke_args<'a>(&'a self, action: &'a str, input: &'a str) -> Vec<&'a str> {
        vec![
            "cap",
            "invoke",
            "--agent",
            AGENT_NAME,
            "--world",
            &self.world_id,
            "--cap",
            &self.capability,
            "--action",
            action,
            "--input",
            input,
        ]
    }

    /// Ship the working tree's changes to the World's session container.
    /// Nothing runs on a stale tree: a failed sync fails the command.
    async fn sync_workspace(&self) -> Result<()> {
        let Some(sync) = &self.sync else {
            return Ok(());
        };
        let mut guard = sync.lock().await;
        let root = guard.root.clone();
        let previous = guard.snapshot.clone();
        let (delta, current) = tokio::task::spawn_blocking(move || -> Result<_> {
            let current = WorkspaceSync::list(&root)?;
            let delta = WorkspaceSync::delta(&root, &previous, &current, SYNC_CHUNK_BYTES)?;
            Ok((delta, current))
        })
        .await
        .context("workspace sync task")??;
        if delta.archives.is_empty() && delta.deletes.is_empty() && guard.initialized {
            return Ok(());
        }
        let mut deletes = delta.deletes.clone();
        let mut requests: Vec<Value> = delta
            .archives
            .iter()
            .map(|archive| json!({"archive_gz_b64": base64::engine::general_purpose::STANDARD.encode(archive)}))
            .collect();
        if requests.is_empty() {
            requests.push(json!({}));
        }
        requests[0]["deletes"] = Value::from(std::mem::take(&mut deletes));
        for request in &requests {
            let input = request.to_string();
            let result = run_json(
                &self.cli.binary,
                &self.cli.home,
                &self.invoke_args("sync", &input),
            )
            .await
            .context("ShannonNet workspace sync failed")?;
            if let Some(err) = result.pointer("/response/error").and_then(Value::as_str)
                && !err.is_empty()
            {
                bail!("ShannonNet workspace sync refused: {err}");
            }
        }
        guard.snapshot = current;
        guard.initialized = true;
        Ok(())
    }

    /// The Task World this session invokes in (for receipts and inspection).
    #[must_use]
    pub fn world_id(&self) -> &str {
        &self.world_id
    }
}

impl Drop for ShannonBackend {
    /// Best-effort teardown of the worker's session container. Detached: a
    /// backend may drop inside an async context and must not block; the
    /// worker's idle reaper covers the case where this never runs.
    fn drop(&mut self) {
        if self.sync.is_none() {
            return;
        }
        let args = self.invoke_args("destroy", "{}");
        let _ = std::process::Command::new(&self.cli.binary)
            .args(cli_args(&self.cli.home, &args))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn();
    }
}

/// Stamp of one workspace file: enough to notice a change cheaply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Stamp {
    len: u64,
    mtime: SystemTime,
}

/// What one sync must ship: gzip tar archives (chunked) and deletions.
#[derive(Debug, Default)]
struct SyncDelta {
    archives: Vec<Vec<u8>>,
    deletes: Vec<String>,
}

/// Tracks what the worker's session has already received.
#[derive(Debug)]
struct WorkspaceSync {
    root: PathBuf,
    snapshot: HashMap<PathBuf, Stamp>,
    initialized: bool,
}

impl WorkspaceSync {
    fn new(root: PathBuf) -> Self {
        Self {
            root,
            snapshot: HashMap::new(),
            initialized: false,
        }
    }

    /// Regular files under root that are not ignored (`.gitignore`, global
    /// and local excludes, `.git` itself). Symlinks and oversized files are
    /// skipped: the worker refuses links and the size budget is finite.
    fn list(root: &Path) -> Result<HashMap<PathBuf, Stamp>> {
        let mut out = HashMap::new();
        let walker = ignore::WalkBuilder::new(root)
            .hidden(false)
            .git_ignore(true)
            .git_global(true)
            .git_exclude(true)
            .require_git(false)
            .filter_entry(|entry| entry.file_name() != ".git")
            .build();
        for entry in walker {
            let entry = entry.context("walking workspace")?;
            let Some(ft) = entry.file_type() else {
                continue;
            };
            if !ft.is_file() {
                continue;
            }
            let meta = entry.metadata().context("workspace file metadata")?;
            if meta.len() > SYNC_MAX_FILE_BYTES {
                continue;
            }
            let rel = entry
                .path()
                .strip_prefix(root)
                .context("workspace path outside root")?
                .to_path_buf();
            out.insert(
                rel,
                Stamp {
                    len: meta.len(),
                    mtime: meta.modified().unwrap_or(SystemTime::UNIX_EPOCH),
                },
            );
        }
        Ok(out)
    }

    /// Archives for files that are new or changed since `previous`, chunked
    /// at `chunk_bytes` of raw content, plus the paths that disappeared.
    fn delta(
        root: &Path,
        previous: &HashMap<PathBuf, Stamp>,
        current: &HashMap<PathBuf, Stamp>,
        chunk_bytes: u64,
    ) -> Result<SyncDelta> {
        let mut changed: Vec<&PathBuf> = current
            .iter()
            .filter(|(path, stamp)| previous.get(*path) != Some(*stamp))
            .map(|(path, _)| path)
            .collect();
        changed.sort();
        let mut deletes: Vec<String> = previous
            .keys()
            .filter(|path| !current.contains_key(*path))
            .map(|path| path.to_string_lossy().replace('\\', "/"))
            .collect();
        deletes.sort();

        let mut archives = Vec::new();
        let mut builder: Option<tar::Builder<flate2::write::GzEncoder<Vec<u8>>>> = None;
        let mut raw = 0u64;
        for path in changed {
            let len = current[path].len;
            if builder.is_some() && raw + len > chunk_bytes {
                archives.push(finish_archive(builder.take().unwrap())?);
                raw = 0;
            }
            let b = builder.get_or_insert_with(|| {
                tar::Builder::new(flate2::write::GzEncoder::new(
                    Vec::new(),
                    flate2::Compression::fast(),
                ))
            });
            let name = path.to_string_lossy().replace('\\', "/");
            b.append_path_with_name(root.join(path), &name)
                .with_context(|| format!("archiving {name}"))?;
            raw += len;
        }
        if let Some(b) = builder {
            archives.push(finish_archive(b)?);
        }
        Ok(SyncDelta { archives, deletes })
    }
}

fn finish_archive(builder: tar::Builder<flate2::write::GzEncoder<Vec<u8>>>) -> Result<Vec<u8>> {
    let mut gz = builder.into_inner().context("finishing archive")?;
    gz.flush().context("flushing archive")?;
    gz.finish().context("compressing archive")
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
        self.sync_workspace().await?;
        let timeout_ms = (self.timeout_secs * 1000).min(MAX_WORKER_TIMEOUT_MS);
        let input = json!({"command": cmd, "env": env, "timeout_ms": timeout_ms}).to_string();
        let result = run_json(
            &self.cli.binary,
            &self.cli.home,
            &self.invoke_args("invoke", &input),
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
    action=invoke; input=""
    while [ $# -gt 0 ]; do
      case "$1" in --action) action=$2; shift ;; --input) input=$2; shift ;; esac
      shift
    done
    if [ "$action" = "sync" ]; then printf '{{"response":{{"output":{{"session_id":"s1","files_written":1}}}}}}'; exit 0; fi
    if [ "$action" = "destroy" ]; then printf '{{"response":{{"output":{{"destroyed":true}}}}}}'; exit 0; fi
    # echo the command back through the docker-kind result shape
    cmd=$(printf '%s' "$input" | sed -n 's/.*"command":"\([^"]*\)".*/\1/p')
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
            false,
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
                "cap invoke --agent codewhale --world world-1 --cap cap://sandbox/exec --action invoke --input"
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
            false,
        )
        .unwrap();
        let log = std::fs::read_to_string(dir.path().join("argv.log")).unwrap();
        assert!(!log.contains("agent create"), "{log}");
        assert!(log.contains("--actions invoke capability cap://x"), "{log}");
    }

    /// Entries of every sync archive found in the fake CLI's argv log, in
    /// order, plus the deletes each sync carried.
    #[cfg(unix)]
    fn synced(log_path: &Path) -> Vec<(Vec<String>, Vec<String>)> {
        use std::io::Read;
        let log = std::fs::read_to_string(log_path).unwrap();
        log.lines()
            .filter(|l| l.contains("--action sync --input "))
            .map(|l| {
                let input: Value =
                    serde_json::from_str(l.split_once("--input ").unwrap().1).unwrap();
                let deletes = input["deletes"]
                    .as_array()
                    .map(|d| d.iter().map(|v| v.as_str().unwrap().to_string()).collect())
                    .unwrap_or_default();
                let mut names = Vec::new();
                if let Some(b64) = input["archive_gz_b64"].as_str() {
                    let raw = base64::engine::general_purpose::STANDARD
                        .decode(b64)
                        .unwrap();
                    let mut archive =
                        tar::Archive::new(flate2::read::GzDecoder::new(raw.as_slice()));
                    for entry in archive.entries().unwrap() {
                        let mut entry = entry.unwrap();
                        let mut content = String::new();
                        entry.read_to_string(&mut content).unwrap();
                        names.push(format!("{}={content}", entry.path().unwrap().display()));
                    }
                }
                (names, deletes)
            })
            .collect()
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn sync_ships_full_tree_then_only_changes() {
        let dir = tempfile::tempdir().unwrap();
        let binary = fake_shannon(dir.path(), true);
        let ws = dir.path().join("ws");
        std::fs::create_dir_all(ws.join("sub")).unwrap();
        std::fs::create_dir_all(ws.join(".git")).unwrap();
        std::fs::write(ws.join(".git/HEAD"), "ref: refs/heads/main").unwrap();
        std::fs::write(ws.join(".gitignore"), "ignored.txt\ntarget/\n").unwrap();
        std::fs::write(ws.join("a.txt"), "one").unwrap();
        std::fs::write(ws.join("sub/b.txt"), "two").unwrap();
        std::fs::write(ws.join("ignored.txt"), "never").unwrap();
        std::fs::create_dir_all(ws.join("target")).unwrap();
        std::fs::write(ws.join("target/big.o"), "never").unwrap();
        std::os::unix::fs::symlink("/etc/hosts", ws.join("link")).unwrap();
        let backend = ShannonBackend::new(
            binary,
            dir.path().join("home"),
            DEFAULT_CAPABILITY.into(),
            &ws,
            30,
            true,
        )
        .unwrap();
        let log = dir.path().join("argv.log");
        assert!(
            std::fs::read_to_string(&log)
                .unwrap()
                .contains("--actions invoke,sync,destroy capability")
        );

        // First command: the whole (non-ignored, non-git, non-link) tree.
        backend.exec("ls", &HashMap::new()).await.unwrap();
        let syncs = synced(&log);
        assert_eq!(syncs.len(), 1, "{syncs:?}");
        let mut names = syncs[0].0.clone();
        names.sort();
        assert_eq!(
            names,
            vec![
                ".gitignore=ignored.txt\ntarget/\n",
                "a.txt=one",
                "sub/b.txt=two"
            ]
        );
        assert!(syncs[0].1.is_empty());

        // Unchanged tree: no sync at all.
        backend.exec("ls", &HashMap::new()).await.unwrap();
        assert_eq!(synced(&log).len(), 1);

        // One edit and one deletion: exactly those travel.
        std::thread::sleep(std::time::Duration::from_millis(20));
        std::fs::write(ws.join("a.txt"), "one-edited").unwrap();
        std::fs::remove_file(ws.join("sub/b.txt")).unwrap();
        backend.exec("ls", &HashMap::new()).await.unwrap();
        let syncs = synced(&log);
        assert_eq!(syncs.len(), 2, "{syncs:?}");
        assert_eq!(syncs[1].0, vec!["a.txt=one-edited"]);
        assert_eq!(syncs[1].1, vec!["sub/b.txt"]);

        // Every sync precedes its command in the log.
        let log_text = std::fs::read_to_string(&log).unwrap();
        let first_sync = log_text.find("--action sync").unwrap();
        let first_invoke = log_text.find("--action invoke").unwrap();
        assert!(first_sync < first_invoke);
    }

    #[test]
    fn delta_chunks_archives_by_raw_size() {
        let dir = tempfile::tempdir().unwrap();
        let ws = dir.path();
        for i in 0..5 {
            std::fs::write(ws.join(format!("f{i}.bin")), vec![b'x'; 100]).unwrap();
        }
        let current = WorkspaceSync::list(ws).unwrap();
        assert_eq!(current.len(), 5);
        let delta = WorkspaceSync::delta(ws, &HashMap::new(), &current, 250).unwrap();
        // 5 x 100 bytes at 250 per chunk: 2 + 2 + 1.
        assert_eq!(delta.archives.len(), 3, "{}", delta.archives.len());
        let unchanged = WorkspaceSync::delta(ws, &current, &current, 250).unwrap();
        assert!(unchanged.archives.is_empty() && unchanged.deletes.is_empty());
    }

    #[test]
    fn missing_binary_is_a_construction_error() {
        let err = ShannonBackend::new(
            PathBuf::from("/nonexistent/shannon-cli"),
            PathBuf::from("/tmp"),
            "cap://x".into(),
            Path::new("/p"),
            5,
            false,
        )
        .unwrap_err();
        assert!(err.to_string().contains("codewhale Agent"), "{err}");
    }
}
