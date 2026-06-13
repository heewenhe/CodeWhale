# Agent Fleet

Agent Fleet is the local-first control plane for durable multi-worker runs. The
initial CLI surface is:

```sh
codewhale fleet init
codewhale fleet run tasks.json --max-workers 4
codewhale fleet status
codewhale fleet inspect <worker-id>
codewhale fleet interrupt <worker-id>
codewhale fleet restart <worker-id>
codewhale fleet stop --all
```

Fleet state is stored under the workspace in `.codewhale/fleet.jsonl`. Worker
logs and adapter logs are stored under `.codewhale/fleet/` and
`.codewhale/fleet-host/`.

## Task Spec

`codewhale fleet run` accepts JSON or TOML. A minimal JSON spec:

```json
{
  "name": "local smoke",
  "tasks": [
    {
      "id": "lint",
      "name": "Lint",
      "instructions": "Run the lint check and report failures.",
      "expected_artifacts": ["log"]
    }
  ]
}
```

Workers are optional. If omitted, CodeWhale creates local worker slots up to
`--max-workers`.

Task specs are typed in Rust and keep verification data separate from worker
transcripts. A task can declare:

- `id`, `name`, `description`, `objective`, and `instructions`
- `worker` role, tool profile, tools, and required capabilities
- `workspace` root, required files, writable paths, and environment allowlist
- `input_files`, extra `context`, `budget`, `timeout_seconds`, and `retry_policy`
- `expected_artifacts`, `scorer`, `tags`, and free-form `metadata`

Workers write bounded artifact files under `.codewhale/fleet/` and ledger only
the artifact refs: kind, path, checksum, MIME type, and size. Receipts record
`pass`, `fail`, `partial`, `skip`, or `timeout`; failed receipts may also mark
the source as `transport`, `task`, or `verifier`. `codewhale fleet status`
surfaces those failure-source counts separately.

Deterministic built-in scorers are `exit_code`, `file_exists`, `regex_match`,
and `json_path`. Specs may also declare `command`,
`code_whale_verifier_prompt`, or `manual`; those record a partial receipt until
an explicit verifier pass completes.

### Release Triage Example

```json
{
  "name": "v0.8.60 release triage",
  "labels": {
    "milestone": "v0.8.60"
  },
  "tasks": [
    {
      "id": "release-issue-sweep",
      "name": "Release issue sweep",
      "objective": "Find open v0.8.60 blockers and credit-sensitive PRs.",
      "instructions": "Review the v0.8.60 milestone, linked PRs, changelog entries, and contributor-credit requirements. Write a concise blocker report.",
      "worker": {
        "role": "release-triage",
        "tool_profile": "read-only",
        "tools": ["gh", "git"],
        "capabilities": ["github", "release"]
      },
      "workspace": {
        "required_files": ["Cargo.toml", "CHANGELOG.md", ".github/AUTHOR_MAP"],
        "writable_paths": [".codewhale/fleet"],
        "environment": {
          "required": ["PATH"]
        }
      },
      "input_files": ["CHANGELOG.md", ".github/AUTHOR_MAP"],
      "context": ["Treat community PRs as maintainer evidence."],
      "budget": {
        "max_tokens": 12000,
        "max_tool_calls": 24,
        "max_seconds": 900
      },
      "timeout_seconds": 900,
      "expected_artifacts": ["log", "report", "receipt"],
      "scorer": {
        "kind": "exit_code"
      },
      "retry_policy": {
        "max_attempts": 2,
        "initial_backoff_seconds": 10,
        "max_backoff_seconds": 60,
        "backoff_multiplier": 2
      },
      "tags": ["release", "triage"],
      "metadata": {
        "class": "release"
      }
    }
  ]
}
```

### Code Review Swarm Example

```json
{
  "name": "code review swarm",
  "tasks": [
    {
      "id": "protocol-review",
      "name": "Protocol review",
      "objective": "Review fleet protocol changes for compatibility and sparse JSON behavior.",
      "instructions": "Inspect crates/protocol/src/fleet.rs and report behavior regressions, missing serde defaults, or unsafe wire changes.",
      "worker": {
        "role": "reviewer",
        "tool_profile": "read-only",
        "tools": ["git", "rg", "cargo"],
        "capabilities": ["rust"]
      },
      "input_files": ["crates/protocol/src/fleet.rs"],
      "budget": {
        "max_tokens": 8000,
        "max_tool_calls": 16,
        "max_seconds": 600
      },
      "expected_artifacts": ["log", "report", "receipt"],
      "scorer": {
        "kind": "code_whale_verifier_prompt",
        "prompt": "Verify the review includes at least one concrete file:line finding or explicitly says no issues were found."
      },
      "tags": ["review", "protocol"],
      "metadata": {
        "class": "code-review"
      }
    },
    {
      "id": "tui-review",
      "name": "TUI review",
      "objective": "Review fleet CLI and manager behavior for operator-visible regressions.",
      "instructions": "Inspect crates/tui/src/fleet and crates/tui/src/main.rs. Focus on status output, receipt recording, and failure classification.",
      "worker": {
        "role": "reviewer",
        "tool_profile": "read-only",
        "tools": ["git", "rg", "cargo"],
        "capabilities": ["rust", "cli"]
      },
      "input_files": ["crates/tui/src/fleet", "crates/tui/src/main.rs"],
      "budget": {
        "max_tokens": 10000,
        "max_tool_calls": 20,
        "max_seconds": 600
      },
      "expected_artifacts": ["log", "report", "receipt"],
      "scorer": {
        "kind": "manual"
      },
      "tags": ["review", "tui"],
      "metadata": {
        "class": "code-review"
      }
    }
  ]
}
```

## Host Adapters

The host adapter boundary supports local child processes and explicit SSH
workers. Adapters expose the same operations: start, read status, read bounded
logs, interrupt, restart, stop, and cleanup.

Local workers run as child processes with stdin closed and stdout/stderr written
to bounded fleet host logs. They inherit only a small safe base environment
such as `PATH` and explicitly allowlisted variables.

SSH workers run through the system `ssh` client with `BatchMode=yes` and a
bounded connect timeout. Remote environment variables are sent with OpenSSH
`SendEnv`; values are not embedded in the local ssh argv or fleet logs.

Example SSH worker spec:

```json
{
  "id": "builder-1",
  "name": "Builder 1",
  "host": {
    "kind": "ssh",
    "host": "builder.example.com",
    "user": "codewhale",
    "port": 22,
    "identity": "~/.ssh/codewhale_fleet",
    "working_directory": "/srv/codewhale/work",
    "env_allowlist": ["CODEWHALE_PROFILE"],
    "codewhale_binary": "/usr/local/bin/codewhale"
  },
  "capabilities": ["local", "linux", "tests"],
  "max_concurrent_tasks": 1
}
```

Defaults are intentionally conservative:

- no hosted control plane or cloud provisioning is enabled;
- SSH requires an explicit host, working directory, and CodeWhale binary path;
- secret-like environment names such as `TOKEN`, `SECRET`, `PASSWORD`,
  `API_KEY`, and `PRIVATE_KEY` are rejected from adapter allowlists;
- secrets should remain in CodeWhale config providers or remote host config,
  not in task instructions, argv, or fleet logs.
