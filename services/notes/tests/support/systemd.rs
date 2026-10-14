//! A fake systemd bus that records each call and runs the shipped template units.
//!
//! When the service starts `ratatoskr-notes-login@<id>.service` or
//! `ratatoskr-notes-cleanup@<id>.service`, the fake reads the shipped template under
//! `deploy/systemd/` and runs its `ExecStartPre=` and `ExecStart=` lines, as systemd would, with
//! `%i` replaced, the host paths moved into a temporary directory, and `/usr/bin/ob` replaced by a
//! stub. The test therefore checks the shipped unit.
//! The fake reads `Environment=` and `EnvironmentFile=`, and copies each `LoadCredential=` file
//! into a fresh directory that it names in `$CREDENTIALS_DIRECTORY` and removes after the run.
//!
//! When the service starts `ratatoskr-notes-sync@<id>.service`, the fake runs the `ExecStartPre=`
//! and `ExecStart=` lines of the shipped sync template in the same way. The stub `ob` records
//! `ob sync --continuous` and returns at once, so the start job of the unit is done when its
//! command started, as for a unit of `Type=exec`.
//!
//! The stub `ob` records its arguments and what it reads on standard input. Its `login` writes a
//! token to the client state. It exits 1 when the fake Obsidian account refuses the login, and it
//! ends without a token when the account needs an MFA code and the call gives none. Its
//! `sync-setup` exits 2 when the account refuses the end-to-end password, and 3 when the account
//! has no remote vault with the id.
//!
//! The temporary directory of the fake also stands for the host files that the service reads and
//! writes: the secrets key file and the credentials root. The output of each unit command goes to
//! a journal that a test can search.
//!
//! Before it runs the login unit, the fake records the credentials directory of the connection
//! that the service wrote, so that a test can check the modes and the contents.
//!
//! A test can hold each login unit before its first command, so that the login runs as long as the
//! test needs. A stop of a login unit that runs ends its start job with `canceled` and kills the
//! command that runs, as systemd does.

#![allow(
    clippy::expect_used,
    dead_code,
    reason = "test scaffolding; not every test binary uses every helper"
)]

use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use tokio::sync::{Notify, watch};

use ratatoskr_notes::systemd::{BusError, JobResult, SystemdBus, UnitName};
use uuid::Uuid;

use crate::support::journal::Journals;

/// Where the shipped units keep the per-connection directories on the host.
const HOST_CONNECTIONS_ROOT: &str = "/mnt/nvme/ratatoskr/notes/connections";

/// Where the service writes the credential files of a login on the host.
const HOST_CREDENTIALS_ROOT: &str = "/run/ratatoskr-notes/credentials";

/// Where the shipped units find `ob` on the host.
const HOST_OB: &str = "/usr/bin/ob";

/// The bytes of the test key file: 32 bytes, as `/etc/ratatoskr/notes-secrets.key` holds them.
pub(crate) const KEY_BYTES: [u8; 32] = *b"test-key-of-32-bytes-for-notes!!";

/// The stub `ob`. `@DIR@` is the directory of the fake.
const STUB_OB: &str = r#"#!/bin/sh
dir='@DIR@'
printf '%s\n' "$*" >> "$dir/ob.log"
printf '%s\037' "$@" >> "$dir/ob.args"
printf '\n' >> "$dir/ob.args"
case "$1" in
login)
    cat > "$dir/stdin.login"
    if [ -e "$dir/ob-sleeps" ]; then sleep 1; fi
    if [ -e "$dir/refuse-login" ]; then echo "Login failed." >&2; exit 1; fi
    case " $* " in
    *" --mfa "*) ;;
    *) if [ -e "$dir/need-mfa" ]; then echo "MFA code:"; exit 0; fi ;;
    esac
    mkdir -p "$XDG_CONFIG_HOME/obsidian-headless"
    printf 'FAKE-TOKEN\n' > "$XDG_CONFIG_HOME/obsidian-headless/auth_token"
    echo "Logged in"
    ;;
sync-setup)
    cat > "$dir/stdin.sync-setup"
    if [ -e "$dir/ob-sleeps" ]; then sleep 1; fi
    if [ -e "$dir/refuse-e2e" ]; then echo "Failed to validate password." >&2; exit 2; fi
    if [ -e "$dir/no-vault" ]; then echo "Vault not found." >&2; exit 3; fi
    echo "Setting up..."
    ;;
esac
exit 0
"#;

/// The login units that run now, by unit name.
type Running = Arc<Mutex<HashMap<String, (Arc<Notify>, watch::Receiver<()>)>>>;

/// One call that the service made.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Call {
    Start(String),
    Stop(String),
}

/// One credential file as the fake found it when the login unit started.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CredentialFile {
    pub(crate) name: String,
    pub(crate) mode: u32,
    pub(crate) contents: String,
}

/// The credentials directory of one connection when its login unit started.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CredentialSnapshot {
    pub(crate) directory_mode: u32,
    pub(crate) files: Vec<CredentialFile>,
}

/// The fake bus. Clones share one record.
#[derive(Debug, Clone)]
pub(crate) struct FakeSystemd {
    calls: Arc<Mutex<Vec<Call>>>,
    exit_statuses: Arc<Mutex<Vec<(String, i32)>>>,
    snapshots: Arc<Mutex<Vec<(String, CredentialSnapshot)>>>,
    journal: Arc<Mutex<String>>,
    directory: Arc<tempfile::TempDir>,
    /// `true` while each login unit waits before its first command.
    logins_held: Arc<watch::Sender<bool>>,
    /// The login units that run now: the signal that stops each one, and the channel whose close
    /// tells that it ended.
    running: Running,
    /// The number of calls, so that a test can wait for one.
    calls_made: Arc<watch::Sender<usize>>,
    /// The journals of the sync units.
    journals: Journals,
}

impl FakeSystemd {
    pub(crate) fn new() -> Self {
        let directory = tempfile::tempdir().expect("a temporary directory");
        let stub = directory.path().join("ob");
        std::fs::write(
            &stub,
            STUB_OB.replace("@DIR@", &directory.path().display().to_string()),
        )
        .expect("the stub ob is written");
        std::fs::set_permissions(&stub, std::fs::Permissions::from_mode(0o755))
            .expect("the stub ob is executable");
        for root in ["connections", "credentials", "unit-credentials"] {
            std::fs::create_dir(directory.path().join(root)).expect("a host root is created");
        }
        let key_file = directory.path().join("notes-secrets.key");
        std::fs::write(&key_file, KEY_BYTES).expect("the key file is written");
        std::fs::set_permissions(&key_file, std::fs::Permissions::from_mode(0o400))
            .expect("the key file is mode 0400");
        Self {
            calls: Arc::new(Mutex::new(Vec::new())),
            exit_statuses: Arc::new(Mutex::new(Vec::new())),
            snapshots: Arc::new(Mutex::new(Vec::new())),
            journal: Arc::new(Mutex::new(String::new())),
            directory: Arc::new(directory),
            logins_held: Arc::new(watch::Sender::new(false)),
            running: Arc::new(Mutex::new(HashMap::new())),
            calls_made: Arc::new(watch::Sender::new(0)),
            journals: Journals::default(),
        }
    }

    /// Each login unit waits before its first command until [`Self::release_logins`] or a stop of
    /// the unit. The start of the unit is recorded before the wait.
    pub(crate) fn hold_logins(self) -> Self {
        self.logins_held.send_replace(true);
        self
    }

    /// The login units that wait continue, and the next ones do not wait.
    pub(crate) fn release_logins(&self) {
        self.logins_held.send_replace(false);
    }

    /// The fake Obsidian account refuses the end-to-end password that the owner gives.
    pub(crate) fn refuse_e2e_password(self) -> Self {
        self.flag("refuse-e2e");
        self
    }

    /// The fake Obsidian account refuses the login: `ob login` exits 1.
    pub(crate) fn refuse_login(self) -> Self {
        self.flag("refuse-login");
        self
    }

    /// The fake Obsidian account needs an MFA code: `ob login` without `--mfa` ends without a
    /// token.
    pub(crate) fn need_mfa(self) -> Self {
        self.flag("need-mfa");
        self
    }

    /// The fake Obsidian account has no remote vault: `ob sync-setup` exits 3.
    pub(crate) fn find_no_vault(self) -> Self {
        self.flag("no-vault");
        self
    }

    /// The stub `ob` sleeps one second in `login` and in `sync-setup`, so that a test can look at
    /// the processes while they run.
    pub(crate) fn ob_sleeps(self) -> Self {
        self.flag("ob-sleeps");
        self
    }

    fn flag(&self, name: &str) {
        std::fs::write(self.directory.path().join(name), b"").expect("a stub flag is written");
    }

    /// The test's stand-in for `/etc/ratatoskr/notes-secrets.key`.
    pub(crate) fn key_file(&self) -> PathBuf {
        self.directory.path().join("notes-secrets.key")
    }

    /// The test's stand-in for `/run/ratatoskr-notes/credentials`.
    pub(crate) fn credentials_root(&self) -> PathBuf {
        self.directory.path().join("credentials")
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

    /// The arguments that the stub `ob` received, one list for each call, in order. Unlike
    /// [`Self::ob_invocations`], an empty argument stays an empty word.
    pub(crate) fn ob_arguments(&self) -> Vec<Vec<String>> {
        std::fs::read_to_string(self.directory.path().join("ob.args"))
            .map(|log| {
                log.lines()
                    .map(|line| {
                        line.strip_suffix('\u{1f}')
                            .unwrap_or(line)
                            .split('\u{1f}')
                            .map(str::to_owned)
                            .collect()
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// What the stub `ob` read on standard input for one command, `login` or `sync-setup`.
    pub(crate) fn ob_stdin(&self, command: &str) -> Option<String> {
        std::fs::read_to_string(self.directory.path().join(format!("stdin.{command}"))).ok()
    }

    /// Everything that the commands of the units wrote to stdout and stderr, as the journal of
    /// the units would hold it.
    pub(crate) fn journal(&self) -> String {
        self.journal.lock().expect("the journal").clone()
    }

    /// The credentials directory of a connection as the fake found it when the login unit of the
    /// connection started. `None` when the directory did not exist then.
    pub(crate) fn credential_snapshot(&self, connection_id: Uuid) -> Option<CredentialSnapshot> {
        let instance = connection_id.to_string();
        self.snapshots
            .lock()
            .expect("the snapshots")
            .iter()
            .find(|(name, _)| *name == instance)
            .map(|(_, snapshot)| snapshot.clone())
    }

    fn connections_root(&self) -> PathBuf {
        self.directory.path().join("connections")
    }

    fn record(&self, call: Call) {
        self.calls.lock().expect("the call record").push(call);
        self.calls_made.send_modify(|made| *made += 1);
    }

    /// Wait until the service stops `unit`, for at most 10 seconds. Returns whether it did.
    pub(crate) async fn wait_for_stop(&self, unit: &str) -> bool {
        let stop = Call::Stop(unit.to_owned());
        let mut made = self.calls_made.subscribe();
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while !self.calls().contains(&stop) {
                if made.changed().await.is_err() {
                    return;
                }
            }
        })
        .await
        .is_ok()
    }

    /// The journals of the sync units.
    pub(crate) fn journals(&self) -> &Journals {
        &self.journals
    }

    fn record_exit_status(&self, unit: &str, status: i32) {
        self.exit_statuses
            .lock()
            .expect("the exit statuses")
            .push((unit.to_owned(), status));
    }

    fn take_snapshot(&self, instance: &str) {
        let directory = self.credentials_root().join(instance);
        let Ok(metadata) = std::fs::metadata(&directory) else {
            return;
        };
        let mut files: Vec<CredentialFile> = std::fs::read_dir(&directory)
            .expect("the credentials directory can be listed")
            .map(|entry| {
                let entry = entry.expect("a directory entry");
                CredentialFile {
                    name: entry.file_name().to_string_lossy().into_owned(),
                    mode: entry.metadata().expect("metadata").permissions().mode() & 0o7777,
                    contents: std::fs::read_to_string(entry.path()).expect("a credential file"),
                }
            })
            .collect();
        files.sort_by(|a, b| a.name.cmp(&b.name));
        self.snapshots.lock().expect("the snapshots").push((
            instance.to_owned(),
            CredentialSnapshot {
                directory_mode: metadata.permissions().mode() & 0o7777,
                files,
            },
        ));
    }

    /// Runs a shipped template for one instance, as systemd would run a oneshot unit.
    async fn run_template(&self, template: &str, instance: &str) -> Result<JobResult, BusError> {
        let unit_name = template.replace("@.service", &format!("@{instance}.service"));
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../deploy/systemd")
            .join(template);
        let text =
            std::fs::read_to_string(&path).map_err(|error| BusError::Refused(error.into()))?;
        let unit = Template::parse(&text, |raw: &str| self.resolve(raw, instance));

        let mut environment = unit.environment.clone();
        for file in &unit.environment_files {
            let (optional, file) = file
                .strip_prefix('-')
                .map_or((false, file.as_str()), |rest| (true, rest));
            match std::fs::read_to_string(file) {
                Ok(text) => environment.extend(
                    text.lines()
                        .filter_map(|line| line.split_once('='))
                        .map(|(key, value)| (key.to_owned(), value.to_owned())),
                ),
                Err(_) if optional => {}
                Err(_) => return Ok(JobResult::NotDone("failed".to_owned())),
            }
        }
        let credentials = self
            .directory
            .path()
            .join("unit-credentials")
            .join(&unit_name);
        if !unit.credentials.is_empty() {
            std::fs::create_dir(&credentials).expect("the unit credentials directory");
            std::fs::set_permissions(&credentials, std::fs::Permissions::from_mode(0o700))
                .expect("the unit credentials directory is mode 0700");
            for (name, source) in &unit.credentials {
                // systemd fails the start when a credential cannot be read.
                if std::fs::copy(source, credentials.join(name)).is_err() {
                    let _ = std::fs::remove_dir_all(&credentials);
                    return Ok(JobResult::NotDone("failed".to_owned()));
                }
            }
            environment.push((
                "CREDENTIALS_DIRECTORY".to_owned(),
                credentials.display().to_string(),
            ));
        }

        let result = self
            .run_commands(&unit_name, &unit.commands, &environment)
            .await;
        // systemd removes the credentials of a unit when the unit stops.
        let _ = std::fs::remove_dir_all(&credentials);
        result
    }

    async fn run_commands(
        &self,
        unit_name: &str,
        commands: &[String],
        environment: &[(String, String)],
    ) -> Result<JobResult, BusError> {
        for command in commands {
            let (ignore_failure, command) = command
                .strip_prefix('-')
                .map_or((false, command.as_str()), |rest| (true, rest));
            let words = split_command(command);
            let (program, arguments) = words.split_first().expect("a command");
            // A dropped start job kills the command, as the stop of a unit does.
            let output = tokio::process::Command::new(program)
                .args(arguments)
                .kill_on_drop(true)
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
                if let Some(status) = output.status.code() {
                    self.record_exit_status(unit_name, status);
                }
                return Ok(JobResult::NotDone("failed".to_owned()));
            }
        }
        Ok(JobResult::Done)
    }

    /// `%i`, the host paths and `ob`, moved into the temporary directory.
    fn resolve(&self, raw: &str, instance: &str) -> String {
        raw.replace("%i", instance)
            .replace(
                HOST_CONNECTIONS_ROOT,
                &self.connections_root().display().to_string(),
            )
            .replace(
                HOST_CREDENTIALS_ROOT,
                &self.credentials_root().display().to_string(),
            )
            .replace(
                HOST_OB,
                &self.directory.path().join("ob").display().to_string(),
            )
    }
}

/// The directives of a template unit that the fake runs.
#[derive(Debug, Default)]
struct Template {
    environment: Vec<(String, String)>,
    environment_files: Vec<String>,
    credentials: Vec<(String, String)>,
    /// `ExecStartPre=` lines first, then `ExecStart=` lines, as systemd runs them.
    commands: Vec<String>,
}

impl Template {
    fn parse(text: &str, resolve: impl Fn(&str) -> String) -> Self {
        let mut template = Self::default();
        let mut start = Vec::new();
        for line in text.lines().map(str::trim) {
            if line.starts_with('#') {
                continue;
            }
            let Some((key, value)) = line.split_once('=') else {
                continue;
            };
            let value = resolve(value);
            match key {
                "Environment" => {
                    let (name, value) = value.split_once('=').expect("KEY=value");
                    template
                        .environment
                        .push((name.to_owned(), value.to_owned()));
                }
                "EnvironmentFile" => template.environment_files.push(value),
                "LoadCredential" => {
                    let (name, path) = value.split_once(':').expect("name:path");
                    template
                        .credentials
                        .push((name.to_owned(), path.to_owned()));
                }
                "ExecStartPre" => template.commands.push(value),
                "ExecStart" => start.push(value),
                _ => {}
            }
        }
        template.commands.extend(start);
        template
    }
}

/// Splits an `Exec*=` command line as systemd does for this repository's units: words separated
/// by whitespace, a single- or double-quoted word as one argument, and `$$` as a literal `$`.
fn split_command(command: &str) -> Vec<String> {
    let command = command.replace("$$", "$");
    let mut words = Vec::new();
    let mut word = String::new();
    let mut in_word = false;
    let mut quote: Option<char> = None;
    for character in command.chars() {
        match quote {
            Some(open) if character == open => quote = None,
            Some(_) => word.push(character),
            None if character == '\'' || character == '"' => {
                quote = Some(character);
                in_word = true;
            }
            None if character.is_whitespace() => {
                if in_word {
                    words.push(std::mem::take(&mut word));
                    in_word = false;
                }
            }
            None => {
                word.push(character);
                in_word = true;
            }
        }
    }
    if in_word {
        words.push(word);
    }
    words
}

/// The instance of a unit of one template, `None` for a unit of another template.
fn instance_of<'a>(unit: &'a UnitName, template: &str) -> Option<&'a str> {
    unit.as_str()
        .strip_prefix(template.trim_end_matches(".service"))
        .and_then(|rest| rest.strip_suffix(".service"))
}

/// A login unit that runs. Its drop removes it from the record of the running units and closes
/// its channel, so that a stop that waits for it continues.
struct RunningUnit {
    running: Running,
    name: String,
    stop: Arc<Notify>,
    _ended: watch::Sender<()>,
}

impl RunningUnit {
    fn new(running: &Running, name: &str) -> Self {
        let stop = Arc::new(Notify::new());
        let (ended, receiver) = watch::channel(());
        running
            .lock()
            .expect("the running units")
            .insert(name.to_owned(), (Arc::clone(&stop), receiver));
        Self {
            running: Arc::clone(running),
            name: name.to_owned(),
            stop,
            _ended: ended,
        }
    }
}

impl Drop for RunningUnit {
    fn drop(&mut self) {
        if let Ok(mut running) = self.running.lock() {
            running.remove(&self.name);
        }
    }
}

impl FakeSystemd {
    /// Runs the login unit of one instance: it waits while the logins are held, and a stop of the
    /// unit ends it with `canceled` and kills its command.
    async fn run_login(&self, unit: &UnitName, instance: &str) -> Result<JobResult, BusError> {
        let running = RunningUnit::new(&self.running, unit.as_str());
        let mut held = self.logins_held.subscribe();
        let run = async {
            let _ = held.wait_for(|held| !*held).await;
            self.take_snapshot(instance);
            self.run_template("ratatoskr-notes-login@.service", instance)
                .await
        };
        tokio::select! {
            result = run => result,
            () = running.stop.notified() => Ok(JobResult::NotDone("canceled".to_owned())),
        }
    }
}

impl SystemdBus for FakeSystemd {
    async fn start_unit(&self, unit: &UnitName) -> Result<JobResult, BusError> {
        self.record(Call::Start(unit.as_str().to_owned()));
        if let Some(instance) = instance_of(unit, "ratatoskr-notes-login@.service") {
            return self.run_login(unit, instance).await;
        }
        for template in [
            "ratatoskr-notes-cleanup@.service",
            "ratatoskr-notes-sync@.service",
        ] {
            if let Some(instance) = instance_of(unit, template) {
                return self.run_template(template, instance).await;
            }
        }
        Ok(JobResult::Done)
    }

    async fn stop_unit(&self, unit: &UnitName) -> Result<JobResult, BusError> {
        self.journals.record_stop(unit.as_str());
        self.record(Call::Stop(unit.as_str().to_owned()));
        let running = self
            .running
            .lock()
            .expect("the running units")
            .get(unit.as_str())
            .cloned();
        // As systemd does, the stop job ends when the unit has stopped.
        if let Some((stop, mut ended)) = running {
            stop.notify_one();
            while ended.changed().await.is_ok() {}
        }
        Ok(JobResult::Done)
    }

    async fn exit_status(&self, unit: &UnitName) -> Result<Option<i32>, BusError> {
        Ok(self
            .exit_statuses
            .lock()
            .expect("the exit statuses")
            .iter()
            .rev()
            .find(|(name, _)| name == unit.as_str())
            .map(|(_, status)| *status))
    }
}
