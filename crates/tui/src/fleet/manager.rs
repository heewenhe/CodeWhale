//! Local-first fleet manager loop and operator controls.
//!
//! This module is intentionally ledger-first: the first manager can run in the
//! foreground and coordinate logical local workers while later host adapters
//! add real process and SSH execution behind the same records.

#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, SecondsFormat, Utc};
use codewhale_protocol::fleet::*;
use serde_json::Value;
use uuid::Uuid;

use super::ledger::{FleetLedger, FleetLedgerState, FleetTaskLedgerStatus, FleetTaskState};
use super::task_spec::{
    FleetTaskSpecDocument, FleetTaskVerificationInput, load_task_spec_document,
    record_verification_receipt, validate_task_spec_document, verify_task_result,
};

const DEFAULT_STALE_AFTER_SECONDS: u64 = 300;

#[derive(Debug)]
pub struct FleetManager {
    workspace: PathBuf,
    ledger: FleetLedger,
    stale_after: Duration,
}

#[derive(Debug, Clone)]
pub struct FleetRunReport {
    pub run_id: FleetRunId,
    pub task_count: usize,
    pub leased: usize,
    pub queued: usize,
    pub worker_ids: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct FleetTickReport {
    pub leased: usize,
    pub heartbeats: usize,
}

#[derive(Debug, Clone, Default)]
pub struct FleetStatusSnapshot {
    pub runs: usize,
    pub queued: usize,
    pub running: usize,
    pub completed: usize,
    pub partial: usize,
    pub failed: usize,
    pub transport_failed: usize,
    pub task_failed: usize,
    pub verifier_failed: usize,
    pub cancelled: usize,
    pub stale: usize,
    pub workers: BTreeMap<String, FleetWorkerStatus>,
}

#[derive(Debug, Clone)]
pub struct FleetWorkerInspection {
    pub worker_id: String,
    pub status: FleetWorkerStatus,
    pub current_run_id: Option<FleetRunId>,
    pub current_task_id: Option<String>,
    pub latest_heartbeat_at: Option<String>,
    pub latest_event: Option<FleetWorkerEvent>,
    pub artifacts: Vec<FleetArtifactRef>,
    pub last_error: Option<String>,
}

impl FleetManager {
    pub fn open(workspace: impl AsRef<Path>) -> Result<Self> {
        let workspace = workspace.as_ref().to_path_buf();
        let ledger = FleetLedger::open(&workspace)?;
        Ok(Self {
            workspace,
            ledger,
            stale_after: Duration::from_secs(DEFAULT_STALE_AFTER_SECONDS),
        })
    }

    pub fn with_stale_after(mut self, stale_after: Duration) -> Self {
        self.stale_after = stale_after;
        self
    }

    pub fn ledger_path(&self) -> &Path {
        self.ledger.path()
    }

    pub fn load_task_spec(path: &Path) -> Result<FleetTaskSpecDocument> {
        load_task_spec_document(path)
    }

    pub fn create_run_from_task_spec_path(
        &self,
        path: &Path,
        max_workers: usize,
    ) -> Result<FleetRunReport> {
        let doc = Self::load_task_spec(path)?;
        self.create_run(doc, max_workers)
    }

    pub fn create_run(
        &self,
        mut doc: FleetTaskSpecDocument,
        max_workers: usize,
    ) -> Result<FleetRunReport> {
        validate_task_spec_document(&doc)?;
        let max_workers = max_workers.clamp(1, 128);
        let run_id = FleetRunId::from(format!(
            "fleet-{}",
            &Uuid::new_v4().simple().to_string()[..8]
        ));
        let now = timestamp();
        if doc.workers.is_empty() {
            doc.workers = default_local_workers(&run_id, max_workers);
        }
        let run = FleetRun {
            id: run_id.clone(),
            name: doc.name.unwrap_or_else(|| run_id.0.clone()),
            status: FleetRunStatus::Queued,
            task_specs: doc.tasks.clone(),
            worker_specs: doc.workers.clone(),
            labels: doc.labels,
            created_at: now.clone(),
            updated_at: Some(now.clone()),
            completed_at: None,
        };
        self.ledger.create_run(&run)?;
        for task in &run.task_specs {
            self.ledger.enqueue(FleetInboxEntry {
                run_id: run.id.clone(),
                task_id: task.id.clone(),
                priority: task_priority(task),
                enqueued_at: now.clone(),
                lease_deadline: None,
                attempts: 0,
            })?;
        }
        let initial_status = if run.task_specs.is_empty() {
            FleetRunStatus::Completed
        } else {
            FleetRunStatus::Running
        };
        self.ledger
            .update_run_status(&run.id, initial_status, &timestamp())?;
        let tick = self.schedule_run(&run.id, max_workers)?;
        self.refresh_run_status(&run.id)?;
        let state = self.ledger.rebuild_state()?;
        let snapshot = self.status_from_state(Some(&run.id), &state);
        Ok(FleetRunReport {
            run_id: run.id,
            task_count: run.task_specs.len(),
            leased: tick.leased,
            queued: snapshot.queued,
            worker_ids: run.worker_specs.iter().map(|w| w.id.clone()).collect(),
        })
    }

    pub fn schedule_run(&self, run_id: &FleetRunId, max_workers: usize) -> Result<FleetTickReport> {
        let max_workers = max_workers.clamp(1, 128);
        let mut report = FleetTickReport::default();
        let state = self.ledger.rebuild_state()?;
        let run = state
            .runs
            .get(&run_id.0)
            .cloned()
            .ok_or_else(|| anyhow!("fleet run {} does not exist", run_id.0))?;
        let worker_ids = worker_ids_for_run(&run, max_workers);

        for task in active_tasks_for_run(&state, run_id) {
            if let Some(worker_id) = task.leased_to.as_deref()
                && worker_ids.iter().any(|id| id == worker_id)
            {
                self.ledger.heartbeat(worker_id, &timestamp(), None, None)?;
                report.heartbeats += 1;
            }
        }

        loop {
            let state = self.ledger.rebuild_state()?;
            let active_workers = active_workers_for_run(&state, run_id);
            if active_workers.len() >= max_workers {
                break;
            }
            let Some(worker_id) = worker_ids
                .iter()
                .find(|id| !active_workers.contains(*id))
                .cloned()
            else {
                break;
            };
            let Some((entry, task_spec)) = next_enqueued_task_for_run(&state, run_id) else {
                break;
            };
            self.start_worker_task(&worker_id, &entry, &task_spec)?;
            report.leased += 1;
        }

        self.refresh_run_status(run_id)?;
        Ok(report)
    }

    pub fn status(&self) -> Result<FleetStatusSnapshot> {
        let state = self.ledger.rebuild_state()?;
        Ok(self.status_from_state(None, &state))
    }

    pub fn run_status(&self, run_id: &FleetRunId) -> Result<FleetStatusSnapshot> {
        let state = self.ledger.rebuild_state()?;
        Ok(self.status_from_state(Some(run_id), &state))
    }

    pub fn run_has_open_work(&self, run_id: &FleetRunId) -> Result<bool> {
        let status = self.run_status(run_id)?;
        Ok(status.queued + status.running + status.stale > 0)
    }

    pub fn inspect_worker(&self, worker_id: &str) -> Result<FleetWorkerInspection> {
        let state = self.ledger.rebuild_state()?;
        let latest_event = latest_event_for_worker(&state, worker_id).cloned();
        let current = active_task_for_worker(&state, worker_id)
            .or_else(|| latest_task_for_worker(&state, worker_id));
        let artifacts = state
            .artifact_events
            .values()
            .filter(|event| event.worker_id == worker_id)
            .filter_map(|event| match &event.payload {
                FleetWorkerEventPayload::Artifact(artifact) => Some(artifact.clone()),
                _ => None,
            })
            .chain(
                state
                    .receipts
                    .values()
                    .filter(|receipt| receipt.worker_id == worker_id)
                    .flat_map(|receipt| receipt.artifacts.clone()),
            )
            .collect();
        let last_error = latest_error_for_worker(&state, worker_id);
        let status = state
            .workers
            .get(worker_id)
            .cloned()
            .unwrap_or(FleetWorkerStatus::Unknown);
        let latest_heartbeat_at = state
            .heartbeats
            .get(worker_id)
            .map(|heartbeat| heartbeat.timestamp.clone());
        Ok(FleetWorkerInspection {
            worker_id: worker_id.to_string(),
            status,
            current_run_id: current.as_ref().map(|task| task.entry.run_id.clone()),
            current_task_id: current.map(|task| task.entry.task_id.clone()),
            latest_heartbeat_at,
            latest_event,
            artifacts,
            last_error,
        })
    }

    pub fn interrupt_worker(&self, worker_id: &str) -> Result<FleetWorkerInspection> {
        let state = self.ledger.rebuild_state()?;
        let Some(task) = active_task_for_worker(&state, worker_id) else {
            bail!("worker {worker_id} has no running fleet task");
        };
        self.append_worker_event(
            &task.entry.run_id,
            worker_id,
            &task.entry.task_id,
            FleetWorkerEventPayload::Interrupted {
                signal: Some("operator".to_string()),
            },
        )?;
        self.append_worker_event(
            &task.entry.run_id,
            worker_id,
            &task.entry.task_id,
            FleetWorkerEventPayload::Cancelled {
                cancelled_by: Some("operator".to_string()),
            },
        )?;
        self.refresh_run_status(&task.entry.run_id)?;
        self.inspect_worker(worker_id)
    }

    pub fn restart_worker(&self, worker_id: &str) -> Result<FleetWorkerInspection> {
        let state = self.ledger.rebuild_state()?;
        let Some(task) = active_task_for_worker(&state, worker_id)
            .or_else(|| latest_task_for_worker(&state, worker_id))
        else {
            bail!("worker {worker_id} has no fleet task to restart");
        };
        let now = timestamp();
        self.ledger.lease_task(
            &task.entry.run_id,
            &task.entry.task_id,
            worker_id,
            &now,
            None,
        )?;
        self.append_worker_event(
            &task.entry.run_id,
            worker_id,
            &task.entry.task_id,
            FleetWorkerEventPayload::Restarted { restart_count: 1 },
        )?;
        self.append_worker_event(
            &task.entry.run_id,
            worker_id,
            &task.entry.task_id,
            FleetWorkerEventPayload::Running,
        )?;
        self.ledger.heartbeat(worker_id, &timestamp(), None, None)?;
        self.ledger
            .update_run_status(&task.entry.run_id, FleetRunStatus::Running, &timestamp())?;
        self.inspect_worker(worker_id)
    }

    pub fn stop_all(&self) -> Result<usize> {
        let state = self.ledger.rebuild_state()?;
        let now = timestamp();
        let mut affected_runs = BTreeSet::new();
        let mut stopped = 0usize;
        for task in state.tasks.values() {
            if !matches!(
                task.status,
                FleetTaskLedgerStatus::Enqueued | FleetTaskLedgerStatus::Leased
            ) {
                continue;
            }
            if let Some(worker_id) = task.leased_to.as_deref() {
                self.append_worker_event(
                    &task.entry.run_id,
                    worker_id,
                    &task.entry.task_id,
                    FleetWorkerEventPayload::Interrupted {
                        signal: Some("stop_all".to_string()),
                    },
                )?;
            }
            self.ledger.mark_task_terminal_status(
                &task.entry.run_id,
                &task.entry.task_id,
                task.leased_to.as_deref(),
                &now,
                FleetTaskLedgerStatus::Cancelled,
            )?;
            affected_runs.insert(task.entry.run_id.0.clone());
            stopped += 1;
        }
        for run_id in affected_runs {
            self.ledger.update_run_status(
                &FleetRunId::from(run_id),
                FleetRunStatus::Cancelled,
                &timestamp(),
            )?;
        }
        Ok(stopped)
    }

    fn start_worker_task(
        &self,
        worker_id: &str,
        entry: &FleetInboxEntry,
        task_spec: &FleetTaskSpec,
    ) -> Result<()> {
        let now = timestamp();
        self.ledger
            .lease_task(&entry.run_id, &entry.task_id, worker_id, &now, None)?;
        self.append_worker_event(
            &entry.run_id,
            worker_id,
            &entry.task_id,
            FleetWorkerEventPayload::Leased {
                lease_expires_at: None,
            },
        )?;
        self.append_worker_event(
            &entry.run_id,
            worker_id,
            &entry.task_id,
            FleetWorkerEventPayload::Starting,
        )?;
        let log_artifact = self.write_log_artifact(&entry.run_id, worker_id, task_spec)?;
        self.append_worker_event(
            &entry.run_id,
            worker_id,
            &entry.task_id,
            FleetWorkerEventPayload::Artifact(log_artifact.clone()),
        )?;
        self.append_worker_event(
            &entry.run_id,
            worker_id,
            &entry.task_id,
            FleetWorkerEventPayload::Running,
        )?;
        self.ledger.heartbeat(worker_id, &timestamp(), None, None)?;
        self.maybe_complete_local_simulation(entry, worker_id, task_spec, log_artifact)
    }

    fn maybe_complete_local_simulation(
        &self,
        entry: &FleetInboxEntry,
        worker_id: &str,
        task_spec: &FleetTaskSpec,
        log_artifact: FleetArtifactRef,
    ) -> Result<()> {
        let Some(result) = local_simulation_result(task_spec) else {
            return Ok(());
        };
        let now = timestamp();
        let (payload, receipt_result, failure_kind, exit_code) = match result {
            FleetLocalSimulationResult::Pass => (
                FleetWorkerEventPayload::Completed {
                    exit_code: Some(0),
                    summary: Some("local fleet smoke task completed".to_string()),
                },
                FleetTaskResult::Pass,
                None,
                Some(0),
            ),
            FleetLocalSimulationResult::Fail => (
                FleetWorkerEventPayload::Failed {
                    reason: "local fleet smoke task failed".to_string(),
                    recoverable: false,
                },
                FleetTaskResult::Fail,
                Some(FleetTaskFailureKind::Task),
                Some(1),
            ),
            FleetLocalSimulationResult::Skip => (
                FleetWorkerEventPayload::Completed {
                    exit_code: Some(0),
                    summary: Some("local fleet smoke task skipped".to_string()),
                },
                FleetTaskResult::Skip,
                None,
                Some(0),
            ),
            FleetLocalSimulationResult::Timeout => (
                FleetWorkerEventPayload::Failed {
                    reason: "local fleet smoke task timed out".to_string(),
                    recoverable: true,
                },
                FleetTaskResult::Timeout,
                Some(FleetTaskFailureKind::Transport),
                None,
            ),
        };
        self.append_worker_event(&entry.run_id, worker_id, &entry.task_id, payload)?;
        let verification_input = FleetTaskVerificationInput {
            run_id: entry.run_id.clone(),
            task_id: entry.task_id.clone(),
            worker_id: worker_id.to_string(),
            exit_code,
            artifacts: vec![log_artifact.clone()],
        };
        if task_spec.scorer.is_some() {
            let verification = verify_task_result(&self.workspace, task_spec, &verification_input);
            let receipt = record_verification_receipt(
                &self.ledger,
                &self.workspace,
                &verification_input,
                verification,
            )?;
            if matches!(
                receipt.result,
                FleetTaskResult::Fail | FleetTaskResult::Timeout
            ) {
                self.ledger.mark_task_terminal_status(
                    &entry.run_id,
                    &entry.task_id,
                    Some(worker_id),
                    &timestamp(),
                    FleetTaskLedgerStatus::Failed,
                )?;
            }
            return Ok(());
        }
        self.ledger.record_receipt(FleetReceipt {
            run_id: entry.run_id.clone(),
            task_id: entry.task_id.clone(),
            worker_id: worker_id.to_string(),
            completed_at: now,
            result: receipt_result,
            failure_kind,
            artifacts: vec![log_artifact],
            score: None,
        })
    }

    fn append_worker_event(
        &self,
        run_id: &FleetRunId,
        worker_id: &str,
        task_id: &str,
        payload: FleetWorkerEventPayload,
    ) -> Result<FleetWorkerEvent> {
        let state = self.ledger.rebuild_state()?;
        let key = event_key(worker_id, &run_id.0, task_id);
        let seq = state.latest_seq.get(&key).copied().unwrap_or(0) + 1;
        let event = FleetWorkerEvent {
            seq,
            run_id: run_id.clone(),
            worker_id: worker_id.to_string(),
            task_id: task_id.to_string(),
            timestamp: timestamp(),
            payload,
            extra: BTreeMap::new(),
        };
        self.ledger.append_event(event.clone())?;
        Ok(event)
    }

    fn write_log_artifact(
        &self,
        run_id: &FleetRunId,
        worker_id: &str,
        task_spec: &FleetTaskSpec,
    ) -> Result<FleetArtifactRef> {
        let rel_path = PathBuf::from(".codewhale")
            .join("fleet")
            .join(safe_path_segment(&run_id.0))
            .join(safe_path_segment(&task_spec.id))
            .join(format!("{}.log", safe_path_segment(worker_id)));
        let abs_path = self.workspace.join(&rel_path);
        if let Some(parent) = abs_path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating fleet artifact dir {}", parent.display()))?;
        }
        let contents = format!(
            "run_id={}\ntask_id={}\ntask_name={}\nworker_id={}\nstatus=started\n",
            run_id.0, task_spec.id, task_spec.name, worker_id
        );
        std::fs::write(&abs_path, contents)
            .with_context(|| format!("writing fleet worker log {}", abs_path.display()))?;
        let size_bytes = std::fs::metadata(&abs_path).ok().map(|m| m.len());
        Ok(FleetArtifactRef {
            kind: FleetArtifactKind::Log,
            path: rel_path,
            checksum: None,
            mime_type: Some("text/plain".to_string()),
            size_bytes,
        })
    }

    fn refresh_run_status(&self, run_id: &FleetRunId) -> Result<()> {
        let state = self.ledger.rebuild_state()?;
        let mut has_queued = false;
        let mut has_running = false;
        let mut has_failed = false;
        let mut has_cancelled = false;
        let mut has_tasks = false;
        for task in state
            .tasks
            .values()
            .filter(|task| task.entry.run_id == *run_id)
        {
            has_tasks = true;
            match task.status {
                FleetTaskLedgerStatus::Enqueued => has_queued = true,
                FleetTaskLedgerStatus::Leased => has_running = true,
                FleetTaskLedgerStatus::Failed => has_failed = true,
                FleetTaskLedgerStatus::Cancelled => has_cancelled = true,
                FleetTaskLedgerStatus::Completed => {}
            }
        }
        let status = if !has_tasks {
            FleetRunStatus::Completed
        } else if has_queued || has_running {
            FleetRunStatus::Running
        } else if has_failed {
            FleetRunStatus::Failed
        } else if has_cancelled {
            FleetRunStatus::Cancelled
        } else {
            FleetRunStatus::Completed
        };
        self.ledger
            .update_run_status(run_id, status, &timestamp())
            .context("updating fleet run status")
    }

    fn status_from_state(
        &self,
        run_filter: Option<&FleetRunId>,
        state: &FleetLedgerState,
    ) -> FleetStatusSnapshot {
        let mut snapshot = FleetStatusSnapshot {
            runs: state.runs.len(),
            workers: state.workers.clone(),
            ..FleetStatusSnapshot::default()
        };
        for task in state.tasks.values() {
            if run_filter.is_some_and(|run_id| task.entry.run_id != *run_id) {
                continue;
            }
            match task.status {
                FleetTaskLedgerStatus::Enqueued => snapshot.queued += 1,
                FleetTaskLedgerStatus::Leased => {
                    if self.task_is_stale(task, state) {
                        snapshot.stale += 1;
                    } else {
                        snapshot.running += 1;
                    }
                }
                FleetTaskLedgerStatus::Completed => snapshot.completed += 1,
                FleetTaskLedgerStatus::Failed => snapshot.failed += 1,
                FleetTaskLedgerStatus::Cancelled => snapshot.cancelled += 1,
            }
        }
        for receipt in state.receipts.values() {
            if run_filter.is_some_and(|run_id| receipt.run_id != *run_id) {
                continue;
            }
            if receipt.result == FleetTaskResult::Partial {
                snapshot.partial += 1;
            }
            match &receipt.failure_kind {
                Some(FleetTaskFailureKind::Transport) => snapshot.transport_failed += 1,
                Some(FleetTaskFailureKind::Task) => snapshot.task_failed += 1,
                Some(FleetTaskFailureKind::Verifier) => snapshot.verifier_failed += 1,
                None => {}
            }
        }
        snapshot
    }

    fn task_is_stale(&self, task: &FleetTaskState, state: &FleetLedgerState) -> bool {
        let Some(worker_id) = task.leased_to.as_deref() else {
            return true;
        };
        let Some(heartbeat) = state.heartbeats.get(worker_id) else {
            return true;
        };
        let Ok(last) = DateTime::parse_from_rfc3339(&heartbeat.timestamp) else {
            return true;
        };
        let age = Utc::now().signed_duration_since(last.with_timezone(&Utc));
        age.to_std()
            .is_ok_and(|duration| duration > self.stale_after)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FleetLocalSimulationResult {
    Pass,
    Fail,
    Skip,
    Timeout,
}

fn default_local_workers(run_id: &FleetRunId, max_workers: usize) -> Vec<FleetWorkerSpec> {
    (1..=max_workers)
        .map(|index| FleetWorkerSpec {
            id: format!("{}-local-{}", run_id.0, index),
            name: format!("Local worker {index}"),
            host: FleetHostSpec::Local,
            labels: BTreeMap::new(),
            capabilities: vec!["local".to_string()],
            max_concurrent_tasks: Some(1),
        })
        .collect()
}

fn worker_ids_for_run(run: &FleetRun, max_workers: usize) -> Vec<String> {
    run.worker_specs
        .iter()
        .take(max_workers)
        .map(|worker| worker.id.clone())
        .collect()
}

fn active_workers_for_run(state: &FleetLedgerState, run_id: &FleetRunId) -> BTreeSet<String> {
    active_tasks_for_run(state, run_id)
        .filter_map(|task| task.leased_to.clone())
        .collect()
}

fn active_tasks_for_run<'a>(
    state: &'a FleetLedgerState,
    run_id: &'a FleetRunId,
) -> impl Iterator<Item = &'a FleetTaskState> {
    state.tasks.values().filter(move |task| {
        task.entry.run_id == *run_id && matches!(task.status, FleetTaskLedgerStatus::Leased)
    })
}

fn active_task_for_worker<'a>(
    state: &'a FleetLedgerState,
    worker_id: &str,
) -> Option<&'a FleetTaskState> {
    state.tasks.values().find(|task| {
        task.leased_to.as_deref() == Some(worker_id)
            && matches!(task.status, FleetTaskLedgerStatus::Leased)
    })
}

fn latest_task_for_worker<'a>(
    state: &'a FleetLedgerState,
    worker_id: &str,
) -> Option<&'a FleetTaskState> {
    state
        .tasks
        .values()
        .filter(|task| task.leased_to.as_deref() == Some(worker_id))
        .max_by_key(|task| task.completed_at.as_deref().or(task.leased_at.as_deref()))
}

fn next_enqueued_task_for_run(
    state: &FleetLedgerState,
    run_id: &FleetRunId,
) -> Option<(FleetInboxEntry, FleetTaskSpec)> {
    let run = state.runs.get(&run_id.0)?;
    let task = state
        .tasks
        .values()
        .filter(|task| {
            task.entry.run_id == *run_id && matches!(task.status, FleetTaskLedgerStatus::Enqueued)
        })
        .min_by_key(|task| {
            (
                task.entry.priority,
                task.entry.enqueued_at.clone(),
                task.entry.task_id.clone(),
            )
        })?;
    let task_spec = run
        .task_specs
        .iter()
        .find(|spec| spec.id == task.entry.task_id)
        .cloned()?;
    Some((task.entry.clone(), task_spec))
}

fn latest_event_for_worker<'a>(
    state: &'a FleetLedgerState,
    worker_id: &str,
) -> Option<&'a FleetWorkerEvent> {
    state
        .latest_events
        .values()
        .filter(|event| event.worker_id == worker_id)
        .max_by_key(|event| event.seq)
}

fn latest_error_for_worker(state: &FleetLedgerState, worker_id: &str) -> Option<String> {
    state
        .latest_events
        .values()
        .filter(|event| event.worker_id == worker_id)
        .filter_map(|event| match &event.payload {
            FleetWorkerEventPayload::Failed { reason, .. } => {
                Some((event.seq, format!("failed: {reason}")))
            }
            FleetWorkerEventPayload::Cancelled { cancelled_by } => Some((
                event.seq,
                cancelled_by
                    .as_ref()
                    .map(|by| format!("cancelled by {by}"))
                    .unwrap_or_else(|| "cancelled".to_string()),
            )),
            FleetWorkerEventPayload::Interrupted { signal } => Some((
                event.seq,
                signal
                    .as_ref()
                    .map(|signal| format!("interrupted by {signal}"))
                    .unwrap_or_else(|| "interrupted".to_string()),
            )),
            FleetWorkerEventPayload::Stale { last_heartbeat_at } => Some((
                event.seq,
                last_heartbeat_at
                    .as_ref()
                    .map(|ts| format!("stale since {ts}"))
                    .unwrap_or_else(|| "stale".to_string()),
            )),
            _ => None,
        })
        .max_by_key(|(seq, _)| *seq)
        .map(|(_, message)| message)
}

fn local_simulation_result(task: &FleetTaskSpec) -> Option<FleetLocalSimulationResult> {
    if task
        .metadata
        .get("local_complete")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return Some(FleetLocalSimulationResult::Pass);
    }
    match task
        .metadata
        .get("local_result")
        .and_then(Value::as_str)
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("pass" | "passed" | "ok" | "completed") => Some(FleetLocalSimulationResult::Pass),
        Some("fail" | "failed" | "error") => Some(FleetLocalSimulationResult::Fail),
        Some("skip" | "skipped") => Some(FleetLocalSimulationResult::Skip),
        Some("timeout" | "timed_out") => Some(FleetLocalSimulationResult::Timeout),
        _ => None,
    }
}

fn task_priority(task: &FleetTaskSpec) -> i32 {
    task.metadata
        .get("priority")
        .and_then(Value::as_i64)
        .and_then(|value| i32::try_from(value).ok())
        .unwrap_or(0)
}

fn event_key(worker_id: &str, run_id: &str, task_id: &str) -> String {
    format!("{worker_id}:{run_id}:{task_id}")
}

fn timestamp() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn safe_path_segment(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::TempDir;

    fn task(id: &str) -> FleetTaskSpec {
        FleetTaskSpec {
            id: id.to_string(),
            name: id.to_string(),
            description: None,
            objective: Some(format!("Complete {id}")),
            instructions: format!("do {id}"),
            worker: None,
            workspace: None,
            input_files: Vec::new(),
            context: Vec::new(),
            budget: None,
            tags: Vec::new(),
            expected_artifacts: vec![FleetArtifactKind::Log],
            scorer: None,
            retry_policy: None,
            alert_policy: None,
            timeout_seconds: None,
            metadata: BTreeMap::new(),
        }
    }

    fn task_spec_file(dir: &TempDir, tasks: Vec<FleetTaskSpec>) -> PathBuf {
        let path = dir.path().join("fleet-tasks.json");
        let doc = json!({
            "name": "manager smoke",
            "tasks": tasks,
        });
        std::fs::write(&path, serde_json::to_string_pretty(&doc).unwrap()).unwrap();
        path
    }

    #[test]
    fn fleet_manager_creates_run_and_starts_workers_up_to_cap() {
        let tmp = TempDir::new().unwrap();
        let manager = FleetManager::open(tmp.path()).unwrap();
        let path = task_spec_file(&tmp, vec![task("task-a"), task("task-b"), task("task-c")]);

        let report = manager.create_run_from_task_spec_path(&path, 2).unwrap();

        assert_eq!(report.task_count, 3);
        assert_eq!(report.leased, 2);
        assert_eq!(report.queued, 1);
        assert_eq!(report.worker_ids.len(), 2);
        let status = manager.run_status(&report.run_id).unwrap();
        assert_eq!(status.queued, 1);
        assert_eq!(status.running, 2);
        assert_eq!(status.completed, 0);
    }

    #[test]
    fn fleet_manager_inspect_exposes_heartbeat_artifacts_and_errors() {
        let tmp = TempDir::new().unwrap();
        let manager = FleetManager::open(tmp.path()).unwrap();
        let path = task_spec_file(&tmp, vec![task("task-a")]);
        let report = manager.create_run_from_task_spec_path(&path, 1).unwrap();
        let worker_id = &report.worker_ids[0];

        let inspection = manager.inspect_worker(worker_id).unwrap();
        assert_eq!(inspection.status, FleetWorkerStatus::Busy);
        assert_eq!(inspection.current_task_id.as_deref(), Some("task-a"));
        assert!(inspection.latest_heartbeat_at.is_some());
        assert_eq!(inspection.artifacts.len(), 1);
        assert!(inspection.last_error.is_none());

        let inspection = manager.interrupt_worker(worker_id).unwrap();
        assert_eq!(inspection.status, FleetWorkerStatus::Online);
        assert_eq!(
            inspection.last_error.as_deref(),
            Some("cancelled by operator")
        );
        let status = manager.run_status(&report.run_id).unwrap();
        assert_eq!(status.cancelled, 1);
    }

    #[test]
    fn fleet_manager_restart_and_stop_all_are_ledgered() {
        let tmp = TempDir::new().unwrap();
        let manager = FleetManager::open(tmp.path()).unwrap();
        let path = task_spec_file(&tmp, vec![task("task-a"), task("task-b")]);
        let report = manager.create_run_from_task_spec_path(&path, 1).unwrap();
        let worker_id = &report.worker_ids[0];

        manager.interrupt_worker(worker_id).unwrap();
        let inspection = manager.restart_worker(worker_id).unwrap();
        assert_eq!(inspection.status, FleetWorkerStatus::Busy);
        let status = manager.run_status(&report.run_id).unwrap();
        assert_eq!(status.running, 1);
        assert_eq!(status.queued, 1);

        let stopped = manager.stop_all().unwrap();
        assert_eq!(stopped, 2);
        let status = manager.run_status(&report.run_id).unwrap();
        assert_eq!(status.cancelled, 2);
        assert_eq!(status.running, 0);
    }

    #[test]
    fn fleet_manager_can_record_completed_local_smoke_tasks() {
        let tmp = TempDir::new().unwrap();
        let manager = FleetManager::open(tmp.path()).unwrap();
        let mut completed = task("task-a");
        completed
            .metadata
            .insert("local_result".to_string(), json!("pass"));
        let path = task_spec_file(&tmp, vec![completed]);

        let report = manager.create_run_from_task_spec_path(&path, 1).unwrap();

        let status = manager.run_status(&report.run_id).unwrap();
        assert_eq!(status.completed, 1);
        assert_eq!(status.running, 0);
        let state = manager.ledger.rebuild_state().unwrap();
        assert_eq!(state.receipts.len(), 1);
    }

    #[test]
    fn fleet_task_spec_sample_launches_independent_worker_tasks() {
        let tmp = TempDir::new().unwrap();
        let manager = FleetManager::open(tmp.path()).unwrap();
        let path = task_spec_file(
            &tmp,
            vec![
                task("release-triage"),
                task("risk-review"),
                task("docs-check"),
            ],
        );

        let report = manager.create_run_from_task_spec_path(&path, 2).unwrap();

        assert_eq!(report.task_count, 3);
        assert_eq!(report.leased, 2);
        assert_eq!(report.queued, 1);
        assert_ne!(report.worker_ids[0], report.worker_ids[1]);
        let state = manager.ledger.rebuild_state().unwrap();
        assert!(
            state
                .tasks
                .contains_key(&format!("{}:release-triage", report.run_id.0))
        );
        assert!(
            state
                .tasks
                .contains_key(&format!("{}:risk-review", report.run_id.0))
        );
        assert!(
            state
                .tasks
                .contains_key(&format!("{}:docs-check", report.run_id.0))
        );
    }

    #[test]
    fn fleet_task_spec_local_scorer_records_receipt_artifact() {
        let tmp = TempDir::new().unwrap();
        let manager = FleetManager::open(tmp.path()).unwrap();
        let mut completed = task("task-a");
        completed.scorer = Some(FleetScorerSpec::ExitCode);
        completed
            .metadata
            .insert("local_result".to_string(), json!("pass"));
        let path = task_spec_file(&tmp, vec![completed]);

        let report = manager.create_run_from_task_spec_path(&path, 1).unwrap();

        let status = manager.run_status(&report.run_id).unwrap();
        assert_eq!(status.completed, 1);
        assert_eq!(status.failed, 0);
        assert_eq!(status.partial, 0);
        let state = manager.ledger.rebuild_state().unwrap();
        let receipt = &state.receipts[&format!("{}:task-a", report.run_id.0)];
        assert_eq!(receipt.result, FleetTaskResult::Pass);
        assert_eq!(receipt.failure_kind, None);
        assert!(receipt.score.as_ref().unwrap().value > 0.99);
        assert!(
            receipt
                .artifacts
                .iter()
                .any(|artifact| matches!(artifact.kind, FleetArtifactKind::Receipt))
        );
    }

    #[test]
    fn fleet_task_spec_status_distinguishes_failure_sources() {
        let tmp = TempDir::new().unwrap();
        let manager = FleetManager::open(tmp.path()).unwrap();
        let mut transport = task("transport-failure");
        transport.scorer = Some(FleetScorerSpec::ExitCode);
        transport
            .metadata
            .insert("local_result".to_string(), json!("timeout"));
        let mut task_failed = task("task-failure");
        task_failed.scorer = Some(FleetScorerSpec::ExitCode);
        task_failed
            .metadata
            .insert("local_result".to_string(), json!("fail"));
        let mut verifier_failed = task("verifier-failure");
        verifier_failed.scorer = Some(FleetScorerSpec::RegexMatch {
            path: PathBuf::from("missing.log"),
            pattern: "[".to_string(),
        });
        verifier_failed
            .metadata
            .insert("local_result".to_string(), json!("pass"));
        let path = task_spec_file(&tmp, vec![transport, task_failed, verifier_failed]);

        let report = manager.create_run_from_task_spec_path(&path, 3).unwrap();

        let status = manager.run_status(&report.run_id).unwrap();
        assert_eq!(status.failed, 3);
        assert_eq!(status.transport_failed, 1);
        assert_eq!(status.task_failed, 1);
        assert_eq!(status.verifier_failed, 1);
        assert_eq!(status.running, 0);
    }
}
