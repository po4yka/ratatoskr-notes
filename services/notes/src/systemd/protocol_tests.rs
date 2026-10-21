//! Characterization tests on a private D-Bus; the host system bus is never used.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};

use super::{DbusSystemd, JobResult, SystemdBus, UnitName};
use tokio::io::AsyncBufReadExt as _;
use zbus::OwnedObjectPath;

const JOB: &str = "/org/freedesktop/systemd1/job/1";
const SERVICE: &str = "/org/freedesktop/systemd1/unit/test";
type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Debug, zbus::DBusError)]
#[zbus(prefix = "org.freedesktop.systemd1")]
enum FixtureError {
    NoSuchUnit(String),
}

struct Manager {
    subscribed: AtomicBool,
    unrelated: bool,
    service_path: &'static str,
}

#[zbus::interface(name = "org.freedesktop.systemd1.Manager")]
impl Manager {
    fn subscribe(&self) {
        self.subscribed.store(true, Ordering::SeqCst);
    }

    async fn start_unit(
        &self,
        unit: &str,
        mode: &str,
        #[zbus(connection)] connection: &zbus::Connection,
    ) -> zbus::fdo::Result<OwnedObjectPath> {
        if !self.subscribed.load(Ordering::SeqCst) || mode != "replace" {
            return Err(zbus::fdo::Error::Failed(
                "not subscribed or wrong mode".into(),
            ));
        }
        let path = OwnedObjectPath::try_from(JOB)
            .map_err(|error| zbus::fdo::Error::Failed(error.to_string()))?;
        if self.unrelated {
            let other = OwnedObjectPath::try_from("/org/freedesktop/systemd1/job/2")
                .map_err(|error| zbus::fdo::Error::Failed(error.to_string()))?;
            connection
                .emit_signal(
                    None::<&str>,
                    super::MANAGER_PATH,
                    super::MANAGER_INTERFACE,
                    "JobRemoved",
                    &(2_u32, other, unit, "failed"),
                )
                .await
                .map_err(|error| zbus::fdo::Error::Failed(error.to_string()))?;
        }
        // Emit before the method reply: a late subscription loses this completion.
        connection
            .emit_signal(
                None::<&str>,
                super::MANAGER_PATH,
                super::MANAGER_INTERFACE,
                "JobRemoved",
                &(1_u32, &path, unit, "done"),
            )
            .await
            .map_err(|error| zbus::fdo::Error::Failed(error.to_string()))?;
        Ok(path)
    }

    fn stop_unit(&self, unit: &str, mode: &str) -> Result<OwnedObjectPath, FixtureError> {
        assert!(self.subscribed.load(Ordering::SeqCst));
        assert!(unit.starts_with("ratatoskr-notes-login@"));
        assert_eq!(mode, "replace");
        Err(FixtureError::NoSuchUnit("already absent".into()))
    }

    fn get_unit(&self, unit: &str) -> zbus::fdo::Result<OwnedObjectPath> {
        assert!(unit.starts_with("ratatoskr-notes-login@"));
        OwnedObjectPath::try_from(self.service_path)
            .map_err(|error| zbus::fdo::Error::Failed(error.to_string()))
    }
}

struct Service {
    code: Arc<AtomicI32>,
    status: Arc<AtomicI32>,
}

#[zbus::interface(name = "org.freedesktop.systemd1.Service")]
impl Service {
    #[zbus(property)]
    fn exec_main_code(&self) -> i32 {
        self.code.load(Ordering::SeqCst)
    }
    #[zbus(property)]
    fn exec_main_status(&self) -> i32 {
        self.status.load(Ordering::SeqCst)
    }
}

struct Fixture {
    bus: DbusSystemd,
    code: Arc<AtomicI32>,
    status: Arc<AtomicI32>,
    _server: zbus::Connection,
    _daemon: tokio::process::Child,
    _directory: tempfile::TempDir,
}

impl Fixture {
    async fn new(unrelated: bool) -> TestResult<Self> {
        let directory = tempfile::tempdir()?;
        let mut daemon = tokio::process::Command::new("dbus-daemon")
            .args(["--session", "--nofork", "--print-address=1"])
            .arg(format!(
                "--address=unix:tmpdir={}",
                directory.path().display()
            ))
            .stdout(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        let stdout = daemon.stdout.take().ok_or("missing D-Bus address output")?;
        let mut lines = tokio::io::BufReader::new(stdout).lines();
        let address = tokio::time::timeout(std::time::Duration::from_secs(5), lines.next_line())
            .await??
            .ok_or("D-Bus exited before publishing its address")?;
        let code = Arc::new(AtomicI32::new(super::CLD_EXITED));
        let status = Arc::new(AtomicI32::new(7));
        let server = zbus::connection::Builder::address(address.as_str())
            .name(super::SYSTEMD)
            .serve_at(
                super::MANAGER_PATH,
                Manager {
                    subscribed: AtomicBool::new(false),
                    unrelated,
                    service_path: SERVICE,
                },
            )
            .serve_at(
                SERVICE,
                Service {
                    code: code.clone(),
                    status: status.clone(),
                },
            )
            .build()
            .await?;
        let client = zbus::connection::Builder::address(address.as_str())
            .build()
            .await?;
        Ok(Self {
            bus: DbusSystemd {
                connection: tokio::sync::OnceCell::new_with(Some(client)),
            },
            code,
            status,
            _server: server,
            _daemon: daemon,
            _directory: directory,
        })
    }
}

#[tokio::test]
async fn fast_job_completion_is_observed() -> TestResult {
    let fixture = Fixture::new(false).await?;
    let unit = UnitName::login(uuid::Uuid::nil());
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        fixture.bus.start_unit(&unit),
    )
    .await??;
    assert_eq!(result, JobResult::Done);
    Ok(())
}

#[tokio::test]
async fn unrelated_job_is_ignored() -> TestResult {
    let fixture = Fixture::new(true).await?;
    let unit = UnitName::login(uuid::Uuid::nil());
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        fixture.bus.start_unit(&unit),
    )
    .await??;
    assert_eq!(result, JobResult::Done);
    Ok(())
}

#[tokio::test]
async fn missing_unit_stop_is_done() -> TestResult {
    let fixture = Fixture::new(false).await?;
    let unit = UnitName::login(uuid::Uuid::nil());
    assert_eq!(fixture.bus.stop_unit(&unit).await?, JobResult::Done);
    Ok(())
}

#[tokio::test]
async fn exit_status_is_not_cached() -> TestResult {
    let fixture = Fixture::new(false).await?;
    let unit = UnitName::login(uuid::Uuid::nil());
    assert_eq!(fixture.bus.exit_status(&unit).await?, Some(7));
    fixture.status.store(42, Ordering::SeqCst);
    assert_eq!(fixture.bus.exit_status(&unit).await?, Some(42));
    fixture.code.store(2, Ordering::SeqCst);
    assert_eq!(fixture.bus.exit_status(&unit).await?, None);
    Ok(())
}
