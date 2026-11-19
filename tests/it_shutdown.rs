use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use etcd_client::{Client, WatchOptions, WatchStream, Watcher};

const SHUTDOWN_DEADLINE: Duration = Duration::from_secs(7);

struct Tikvd(Child);

impl Drop for Tikvd {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn_tikvd(pd: &str) -> (Tikvd, String) {
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let child = Command::new(env!("CARGO_BIN_EXE_tikvd"))
        .args(["--listen", &format!("127.0.0.1:{port}"), "--pd", pd])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    (Tikvd(child), format!("http://127.0.0.1:{port}"))
}

async fn open_watch(endpoint: &str, prefix: &str) -> (Watcher, WatchStream) {
    let started = Instant::now();
    loop {
        if let Ok(mut client) = Client::connect([endpoint], None).await
            && let Ok(watch) = client
                .watch(prefix, Some(WatchOptions::new().with_prefix()))
                .await
        {
            return watch;
        }
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "tikvd never served a watch"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn exit_within(child: &mut Child, deadline: Duration) -> Option<ExitStatus> {
    let started = Instant::now();
    while started.elapsed() < deadline {
        if let Some(status) = child.try_wait().unwrap() {
            return Some(status);
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    None
}

async fn signal_with_an_open_watch(signal: &str) {
    let Ok(pd) = std::env::var("TIKVD_TEST_PD") else {
        println!("skipping: TIKVD_TEST_PD is unset");
        return;
    };
    let (mut tikvd, endpoint) = spawn_tikvd(&pd);
    let (_watcher, _stream) =
        open_watch(&endpoint, &format!("it-shutdown-{}/", tikvd.0.id())).await;

    let killed = Command::new("kill")
        .args([&format!("-{signal}"), &tikvd.0.id().to_string()])
        .status()
        .unwrap();
    assert!(killed.success());
    let status = exit_within(&mut tikvd.0, SHUTDOWN_DEADLINE);

    assert!(
        status.is_some_and(|status| status.success()),
        "SIG{signal}: {status:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sigterm_with_an_open_watch_exits_cleanly_within_the_deadline() {
    signal_with_an_open_watch("TERM").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sigint_with_an_open_watch_exits_cleanly_within_the_deadline() {
    signal_with_an_open_watch("INT").await;
}
