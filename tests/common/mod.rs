use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use tikvd::server::{Config, serve_listener};
use tokio::net::TcpListener;

pub fn unique_prefix(label: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{label}-{}-{nanos}-{n}/", std::process::id())
}

pub async fn start_tikvd() -> Option<(etcd_client::Client, SocketAddr)> {
    let Ok(pd) = std::env::var("TIKVD_TEST_PD") else {
        println!("skipping: TIKVD_TEST_PD is unset");
        return None;
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let config = Config {
        listen: addr,
        pd_endpoints: vec![pd],
        tls: None,
    };
    tokio::spawn(serve_listener(listener, config, std::future::pending()));
    let client = etcd_client::Client::connect([format!("http://{addr}")], None)
        .await
        .unwrap();
    Some((client, addr))
}
