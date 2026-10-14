//! A fake systemd bus that records each call and runs the shipped cleanup unit.
//!
//! When the service starts `ratatoskr-notes-cleanup@<id>.service`, the fake reads the shipped
//! template `deploy/systemd/ratatoskr-notes-cleanup@.service` and runs its `ExecStart=` lines with
//! `%i` replaced, the connections root moved into a temporary directory, and `/usr/bin/ob`
//! replaced by a stub that records its arguments. The test therefore checks the shipped unit.
//!
//! The temporary directory of the fake also stands for the host files that the service reads: the
//! secrets key file. The output of each unit command goes to a journal that a test can search.

#![allow(
    clippy::expect_used,
    dead_code,
    reason = "test scaffolding; not every test binary uses every helper"
)]

use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use ratatoskr_notes::systemd::{BusError, JobResult, SystemdBus, UnitName};
use uuid::Uuid;

/// Where the shipped units keep the per-connection directories on the host.
const HOST_CONNECTIONS_ROOT: &str = "/mnt/nvme/ratatoskr/notes/connections";

/// Where the shipped units find `ob` on the host.
const HOST_OB: &str = "/usr/bin/ob";

/// The bytes of the test key file: 32 bytes, as `/etc/ratatoskr/notes-secrets.key` holds them.
pub(crate) const KEY_BYTES: [u8; 32] = *b"test-key-of-32-bytes-for-notes!!";

/// One call that the service made.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Call {
    Start(String),
    Stop(String),
}

/// The fake bus. Clones share one record.
#[derive(Debug, Clone)]
pub(crate) struct FakeSystemd {
    calls: Arc<Mutex<Vec<Call>>>,
    journal: Arc<Mutex<String>>,
    directory: Arc<tempfile::TempDir>,
    refuse_e2e_password: bool,
}

impl FakeSystemd {
    pub(crate) fn new() -> Self {
        let directory = tempfile::tempdir().expect("a temporary directory");
        let stub = directory.path().join("ob");
        let log = directory.path().join("ob.log");
        std::fs::write(
            &stub,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{}'\nexit 0\n",
                log.display()
            ),
        )
        .expect("the stub ob is written");
        std::fs::set_permissions(&stub, std::fs::Permissions::from_mode(0o755))
            .expect("the stub ob is executable");
        std::fs::create_dir(directory.path().join("connections"))
            .expect("the connections root is created");
        let key_file = directory.path().join("notes-secrets.key");
        std::fs::write(&key_file, KEY_BYTES).expect("the key file is written");
        std::fs::set_permissions(&key_file, std::fs::Permissions::from_mode(0o400))
            .expect("the key file is mode 0400");
        Self {
            calls: Arc::new(Mutex::new(Vec::new())),
            journal: Arc::new(Mutex::new(String::new())),
            directory: Arc::new(directory),
            refuse_e2e_password: false,
        }
    }

    /// The fake Obsidian account refuses the end-to-end password that the owner gives.
    pub(crate) fn refuse_e2e_password(mut self) -> Self {
        self.refuse_e2e_password = true;
        self
    }

    /// The test's stand-in for `/etc/ratatoskr/notes-secrets.key`.
    pub(crate) fn key_file(&self) -> PathBuf {
        self.directory.path().join("notes-secrets.key")
    }

    /// Everything that the commands of the units wrote to stdout and stderr, as the journal of
    /// the units would hold it.
    pub(crate) fn journal(&self) -> String {
        self.journal.lock().expect("the journal").clone()
    }

    /// The test's stand-in for `/mnt/nvme/ratatoskr/notes/connections/<id>`.
    pub(crate) fn connection_dir(&self, connection_id: Uuid) -> PathBuf {
        self.connections_root().join(connection_id.to_string())
    }

    /// Every call, in order.
    pub(crate) fn calls(&self) -> Vec<Call> {
        self.calls.lock().expect("the call record").clone()
    }

    /// The argument lines that the stub `ob` received, in order.
    pub(crate) fn ob_invocations(&self) -> Vec<String> {
        std::fs::read_to_string(self.directory.path().join("ob.log"))
            .map(|log| log.lines().map(str::to_owned).collect())
            .unwrap_or_default()
    }

    fn connections_root(&self) -> PathBuf {
        self.directory.path().join("connections")
    }

    fn record(&self, call: Call) {
        self.calls.lock().expect("the call record").push(call);
    }

    /// Runs the `ExecStart=` lines of a shipped template for one instance, as systemd would.
    async fn run_template(&self, template: &str, instance: &str) -> Result<JobResult, BusError> {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../deploy/systemd")
            .join(template);
        let unit =
            std::fs::read_to_string(&path).map_err(|error| BusError::Refused(error.into()))?;
        let root = self.connections_root().display().to_string();
        let ob = self.directory.path().join("ob").display().to_string();
        let resolve = |raw: &str| {
            raw.replace("%i", instance)
                .replace(HOST_CONNECTIONS_ROOT, &root)
                .replace(HOST_OB, &ob)
        };

        let mut environment = Vec::new();
        for line in unit.lines().map(str::trim) {
            if let Some(assignment) = line.strip_prefix("Environment=") {
                let (key, value) = assignment.split_once('=').expect("KEY=value");
                environment.push((key.to_owned(), resolve(value)));
            }
        }
        for line in unit.lines().map(str::trim) {
            let Some(command) = line.strip_prefix("ExecStart=") else {
                continue;
            };
            let (ignore_failure, command) = match command.strip_prefix('-') {
                Some(rest) => (true, rest),
                None => (false, command),
            };
            let words: Vec<String> = command.split_whitespace().map(resolve).collect();
            let (program, arguments) = words.split_first().expect("a command");
            let output = tokio::process::Command::new(program)
                .args(arguments)
                .env_clear()
                .env("PATH", "/usr/bin:/bin")
                .envs(environment.iter().map(|(key, value)| (key, value)))
                .output()
                .await
                .map_err(|error| BusError::Refused(error.into()))?;
            {
                let mut journal = self.journal.lock().expect("the journal");
                journal.push_str(&String::from_utf8_lossy(&output.stdout));
                journal.push_str(&String::from_utf8_lossy(&output.stderr));
            }
            if !output.status.success() && !ignore_failure {
                return Ok(JobResult::NotDone("failed".to_owned()));
            }
        }
        Ok(JobResult::Done)
    }
}

impl SystemdBus for FakeSystemd {
    async fn start_unit(&self, unit: &UnitName) -> Result<JobResult, BusError> {
        self.record(Call::Start(unit.as_str().to_owned()));
        match unit
            .as_str()
            .strip_prefix("ratatoskr-notes-cleanup@")
            .and_then(|rest| rest.strip_suffix(".service"))
        {
            Some(instance) => {
                self.run_template("ratatoskr-notes-cleanup@.service", instance)
                    .await
            }
            None => Ok(JobResult::Done),
        }
    }

    async fn stop_unit(&self, unit: &UnitName) -> Result<JobResult, BusError> {
        self.record(Call::Stop(unit.as_str().to_owned()));
        Ok(JobResult::Done)
    }
}
