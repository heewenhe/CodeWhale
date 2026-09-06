//! `/shannon`: inspect this session's ShannonNet World — the durable Agent,
//! the capabilities projected into the World, children spawned from it, and
//! the receipts on its task. Read-only; the session backend
//! (`sandbox_backend = "shannon"`) leaves a pointer file per workspace that
//! names the ShannonNet home and World.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{Context, bail};
use serde_json::Value;

use codewhale_command_contract::handler::{CommandCapabilities, CommandContexts, CommandHandler};
use codewhale_command_contract::metadata::{CommandInfo, RegisterCommand};

use crate::commands::CommandResult;

pub(in crate::commands) const COMMAND_INFO: CommandInfo = CommandInfo {
    name: "shannon",
    aliases: &[],
    usage: "/shannon [world|trace|children]",
    description_key: "cmd_shannon_description",
};

pub(in crate::commands) struct ShannonCmd;

impl RegisterCommand<CommandResult> for ShannonCmd {
    fn info() -> &'static CommandInfo {
        &COMMAND_INFO
    }

    fn handler() -> CommandHandler<CommandResult> {
        CommandHandler::Contextual {
            capabilities: CommandCapabilities::WORKSPACE,
            handler: shannon_contextual,
        }
    }
}

fn shannon_contextual(contexts: CommandContexts<'_>, arg: Option<&str>) -> CommandResult {
    let parts = contexts.into_parts();
    let Some(workspace) = parts.workspace.as_deref() else {
        return CommandResult::error("Command capability unavailable: workspace");
    };
    match inspect(&workspace.workspace(), arg) {
        Ok(message) => CommandResult::message(message),
        Err(err) => CommandResult::error(err.to_string()),
    }
}

/// What the session backend wrote at session start.
#[derive(Debug)]
struct SessionPointer {
    agent: String,
    world_id: String,
    binary: PathBuf,
    home: PathBuf,
}

fn read_pointer(workspace: &Path) -> anyhow::Result<SessionPointer> {
    let path = crate::sandbox::shannon::session_pointer_path(workspace)
        .context("no Codewhale home directory")?;
    let raw = std::fs::read_to_string(&path).with_context(|| {
        format!(
            "no ShannonNet session for this workspace (set sandbox_backend = \"shannon\" and start a session); expected {}",
            path.display()
        )
    })?;
    let v: Value = serde_json::from_str(&raw).context("session pointer is not JSON")?;
    let field = |name: &str| {
        v.get(name)
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .with_context(|| format!("session pointer lacks {name}"))
    };
    Ok(SessionPointer {
        agent: field("agent")?,
        world_id: field("world_id")?,
        binary: PathBuf::from(field("shannon_binary")?),
        home: PathBuf::from(field("shannon_home")?),
    })
}

fn run_json(p: &SessionPointer, args: &[&str]) -> anyhow::Result<Value> {
    let output = Command::new(&p.binary)
        .arg("--home")
        .arg(&p.home)
        .arg("--json")
        .args(args)
        .stdin(Stdio::null())
        .output()
        .with_context(|| format!("failed to run {}", p.binary.display()))?;
    if !output.status.success() {
        bail!(
            "shannon {}: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    serde_json::from_slice(&output.stdout).context("shannon printed invalid JSON")
}

fn run_text(p: &SessionPointer, args: &[&str]) -> anyhow::Result<String> {
    let output = Command::new(&p.binary)
        .arg("--home")
        .arg(&p.home)
        .args(args)
        .stdin(Stdio::null())
        .output()
        .with_context(|| format!("failed to run {}", p.binary.display()))?;
    if !output.status.success() {
        bail!(
            "shannon {}: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

fn inspect(workspace: &Path, arg: Option<&str>) -> anyhow::Result<String> {
    let pointer = read_pointer(workspace)?;
    let section = arg.map(str::trim).unwrap_or("").to_ascii_lowercase();
    if !matches!(section.as_str(), "" | "world" | "trace" | "children") {
        bail!("Usage: /shannon [world|trace|children]");
    }
    let world = run_json(&pointer, &["world", "inspect", &pointer.world_id])?;
    let mut out = String::new();
    if section.is_empty() || section == "world" {
        out.push_str(&render_world(&pointer, &world));
    }
    if section.is_empty() || section == "children" {
        let tree = run_text(&pointer, &["agent", "tree", &pointer.agent]).unwrap_or_default();
        out.push_str("\nChildren (agent tree):\n");
        out.push_str(tree.trim_end());
        out.push('\n');
    }
    if (section.is_empty() || section == "trace")
        && let Some(task) = world.get("task_id").and_then(Value::as_str)
    {
        let events = run_json(&pointer, &["trace", task])?;
        out.push_str(&render_trace(&events));
    }
    Ok(out.trim_end().to_string())
}

fn render_world(p: &SessionPointer, world: &Value) -> String {
    let field = |name: &str| world.get(name).and_then(Value::as_str).unwrap_or("?");
    let mut out = format!(
        "ShannonNet session\n  agent: {}\n  world: {} ({}, state {})\n  task:  {}\n  home:  {}\n  capabilities projected into this World:\n",
        p.agent,
        field("id"),
        field("name"),
        field("state"),
        field("task_id"),
        p.home.display()
    );
    let attachments = world
        .get("attachments")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if attachments.is_empty() {
        out.push_str("    (none)\n");
    }
    for att in attachments {
        let uri = att.get("uri").and_then(Value::as_str).unwrap_or("?");
        let actions = att
            .get("actions")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(Value::as_str)
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .unwrap_or_default();
        let depth = att
            .get("grant_chain")
            .and_then(Value::as_array)
            .map(Vec::len)
            .unwrap_or(0);
        out.push_str(&format!(
            "    {uri:<28} actions={actions:<22} grant links={depth}\n"
        ));
    }
    out
}

fn render_trace(events: &Value) -> String {
    let events = events.as_array().cloned().unwrap_or_default();
    let shown: Vec<&Value> = events.iter().rev().take(12).collect();
    let mut out = format!(
        "\nReceipts on this task (last {} of {}):\n",
        shown.len(),
        events.len()
    );
    for e in shown.into_iter().rev() {
        let kind = e.get("type").and_then(Value::as_str).unwrap_or("?");
        let at = e.get("created_at").and_then(Value::as_str).unwrap_or("");
        let provider = e.get("provider_id").and_then(Value::as_str).unwrap_or("");
        let capability = e.get("capability").and_then(Value::as_str).unwrap_or("");
        let evidence = e
            .pointer("/data/transport_evidence")
            .and_then(Value::as_str)
            .unwrap_or("");
        let mut line = format!("  {} {kind}", &at[..at.len().min(19)]);
        if !capability.is_empty() {
            line.push_str(&format!(" {capability}"));
        }
        if !provider.is_empty() {
            line.push_str(&format!(" via {provider}"));
        }
        if !evidence.is_empty() {
            line.push_str(&format!(" [{evidence}]"));
        }
        out.push_str(&line);
        out.push('\n');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn renders_world_and_trace() {
        let p = SessionPointer {
            agent: "codewhale".into(),
            world_id: "w1".into(),
            binary: PathBuf::from("shannon"),
            home: PathBuf::from("/h"),
        };
        let world = json!({"id":"w1","name":"codewhale:proj","state":"active","task_id":"t1",
            "attachments":[{"uri":"cap://sandbox/exec","actions":["invoke","sync","destroy"],"grant_chain":[{}]}]});
        let text = render_world(&p, &world);
        assert!(
            text.contains("world: w1 (codewhale:proj, state active)"),
            "{text}"
        );
        let squashed = text.split_whitespace().collect::<Vec<_>>().join(" ");
        assert!(
            squashed.contains("cap://sandbox/exec actions=invoke,sync,destroy grant links=1"),
            "{text}"
        );
        let trace = render_trace(&json!([
            {"type":"capability.invoked","created_at":"2026-09-05T01:02:03.4Z","capability":"cap://sandbox/exec","provider_id":"prov-1","data":{"transport_evidence":"nCTRL tags=tag:shannon-controller"}},
            {"type":"agent.joined","created_at":"2026-09-05T01:03:03.4Z"}
        ]));
        assert!(trace.contains("last 2 of 2"), "{trace}");
        assert!(
            trace.contains("2026-09-05T01:02:03 capability.invoked cap://sandbox/exec via prov-1 [nCTRL tags=tag:shannon-controller]"),
            "{trace}"
        );
        assert!(
            trace.contains("2026-09-05T01:03:03 agent.joined"),
            "{trace}"
        );
    }

    #[test]
    fn missing_pointer_explains_how_to_start_a_session() {
        let err = read_pointer(Path::new("/definitely/not/a/workspace/xyz")).unwrap_err();
        assert!(err.to_string().contains("no ShannonNet session"), "{err}");
    }
}
