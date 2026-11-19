mod common;

use common::{start_tikvd, unique_prefix};
use etcd_client::{
    Compare, CompareOp, DeleteOptions, Error, GetOptions, PutOptions, Txn, TxnOp, TxnOpResponse,
};
use tonic::Code;

fn revision_of(header: Option<&etcd_client::ResponseHeader>) -> i64 {
    header.expect("response carries a header").revision()
}

#[tokio::test]
async fn put_get_delete() {
    let Some((mut client, _)) = start_tikvd().await else {
        return;
    };
    let key = format!("{}a", unique_prefix("it-kv"));

    let put = client.put(key.as_str(), "v1", None).await.unwrap();
    let got = client.get(key.as_str(), None).await.unwrap();
    assert_eq!(got.kvs().len(), 1);
    assert_eq!(got.kvs()[0].value(), b"v1");
    assert_eq!(got.kvs()[0].mod_revision(), revision_of(put.header()));

    let deleted = client.delete(key.as_str(), None).await.unwrap();
    assert_eq!(deleted.deleted(), 1);
    assert!(
        client
            .get(key.as_str(), None)
            .await
            .unwrap()
            .kvs()
            .is_empty()
    );
}

#[tokio::test]
async fn put_and_delete_honour_prev_kv() {
    let Some((mut client, _)) = start_tikvd().await else {
        return;
    };
    let key = format!("{}a", unique_prefix("it-kv"));
    client.put(key.as_str(), "v1", None).await.unwrap();

    let put = client
        .put(key.as_str(), "v2", Some(PutOptions::new().with_prev_key()))
        .await
        .unwrap();
    assert_eq!(put.prev_key().unwrap().value(), b"v1");

    let deleted = client
        .delete(key.as_str(), Some(DeleteOptions::new().with_prev_key()))
        .await
        .unwrap();
    assert_eq!(deleted.prev_kvs().len(), 1);
    assert_eq!(deleted.prev_kvs()[0].value(), b"v2");
}

#[tokio::test]
async fn put_with_ignore_lease_is_unimplemented() {
    let Some((mut client, _)) = start_tikvd().await else {
        return;
    };
    let key = format!("{}a", unique_prefix("it-kv"));
    let error = client
        .put(
            key.as_str(),
            "v",
            Some(PutOptions::new().with_ignore_lease()),
        )
        .await
        .unwrap_err();
    assert_eq!(status_code(&error), Code::Unimplemented);
}

#[tokio::test]
async fn kube_create_txn() {
    let Some((mut client, _)) = start_tikvd().await else {
        return;
    };
    let key = format!("{}a", unique_prefix("it-kv"));
    let create = || {
        Txn::new()
            .when([Compare::mod_revision(key.as_str(), CompareOp::Equal, 0)])
            .and_then([TxnOp::put(key.as_str(), "first", None)])
            .or_else([TxnOp::get(key.as_str(), None)])
    };

    let first = client.txn(create()).await.unwrap();
    assert!(first.succeeded());

    let second = client.txn(create()).await.unwrap();
    assert!(!second.succeeded());
    let responses = second.op_responses();
    let [TxnOpResponse::Get(existing)] = responses.as_slice() else {
        panic!("else branch should hold one range response");
    };
    assert_eq!(existing.kvs().len(), 1);
    assert_eq!(existing.kvs()[0].value(), b"first");
}

#[tokio::test]
async fn kube_update_txn_reports_new_revision() {
    let Some((mut client, _)) = start_tikvd().await else {
        return;
    };
    let key = format!("{}a", unique_prefix("it-kv"));
    let created = client.put(key.as_str(), "v1", None).await.unwrap();

    let update = Txn::new()
        .when([Compare::mod_revision(
            key.as_str(),
            CompareOp::Equal,
            revision_of(created.header()),
        )])
        .and_then([TxnOp::put(key.as_str(), "v2", None)])
        .or_else([TxnOp::get(key.as_str(), None)]);
    let response = client.txn(update).await.unwrap();
    assert!(response.succeeded());
    let responses = response.op_responses();
    let [TxnOpResponse::Put(put)] = responses.as_slice() else {
        panic!("then branch should hold one put response");
    };

    let got = client.get(key.as_str(), None).await.unwrap();
    assert_eq!(got.kvs()[0].value(), b"v2");
    assert_eq!(got.kvs()[0].mod_revision(), revision_of(put.header()));
}

#[tokio::test]
async fn kube_delete_txn() {
    let Some((mut client, _)) = start_tikvd().await else {
        return;
    };
    let key = format!("{}a", unique_prefix("it-kv"));
    let created = client.put(key.as_str(), "v1", None).await.unwrap();

    let delete = Txn::new()
        .when([Compare::mod_revision(
            key.as_str(),
            CompareOp::Equal,
            revision_of(created.header()),
        )])
        .and_then([TxnOp::delete(key.as_str(), None)])
        .or_else([TxnOp::get(key.as_str(), None)]);
    let response = client.txn(delete).await.unwrap();
    assert!(response.succeeded());
    assert!(
        client
            .get(key.as_str(), None)
            .await
            .unwrap()
            .kvs()
            .is_empty()
    );
}

#[tokio::test]
async fn compactor_version_txn() {
    let Some((mut client, _)) = start_tikvd().await else {
        return;
    };
    let key = format!("{}compact", unique_prefix("it-kv"));
    let claim = || {
        Txn::new()
            .when([Compare::version(key.as_str(), CompareOp::Equal, 0)])
            .and_then([TxnOp::put(key.as_str(), "7", None)])
            .or_else([TxnOp::get(key.as_str(), None)])
    };

    assert!(client.txn(claim()).await.unwrap().succeeded());
    let again = client.txn(claim()).await.unwrap();
    assert!(!again.succeeded());
    let responses = again.op_responses();
    let [TxnOpResponse::Get(existing)] = responses.as_slice() else {
        panic!("else branch should hold one range response");
    };
    assert_eq!(existing.kvs()[0].value(), b"7");
}

#[tokio::test]
async fn unsupported_txn_is_unimplemented() {
    let Some((mut client, _)) = start_tikvd().await else {
        return;
    };
    let prefix = unique_prefix("it-kv");
    let (first, second) = (format!("{prefix}a"), format!("{prefix}b"));
    let two_keys = Txn::new()
        .when([
            Compare::mod_revision(first.as_str(), CompareOp::Equal, 0),
            Compare::mod_revision(second.as_str(), CompareOp::Equal, 0),
        ])
        .and_then([
            TxnOp::put(first.as_str(), "x", None),
            TxnOp::put(second.as_str(), "y", None),
        ]);
    let error = client.txn(two_keys).await.unwrap_err();
    assert_eq!(status_code(&error), Code::Unimplemented);
}

#[tokio::test]
async fn prefix_list_with_limit() {
    let Some((mut client, _)) = start_tikvd().await else {
        return;
    };
    let prefix = unique_prefix("it-kv");
    for suffix in ["1", "2", "3"] {
        client
            .put(format!("{prefix}{suffix}"), suffix, None)
            .await
            .unwrap();
    }

    let page = client
        .get(
            prefix.as_str(),
            Some(GetOptions::new().with_prefix().with_limit(2)),
        )
        .await
        .unwrap();
    assert_eq!(page.kvs().len(), 2);
    assert!(page.more());
    assert_eq!(page.count(), 3);
}

#[tokio::test]
async fn future_revision_is_out_of_range() {
    let Some((mut client, _)) = start_tikvd().await else {
        return;
    };
    let key = format!("{}a", unique_prefix("it-kv"));
    let error = client
        .get(
            key.as_str(),
            Some(GetOptions::new().with_revision(i64::MAX)),
        )
        .await
        .unwrap_err();
    assert_eq!(status_code(&error), Code::OutOfRange);
}

#[tokio::test]
async fn compact_is_unimplemented() {
    let Some((mut client, _)) = start_tikvd().await else {
        return;
    };
    let error = client.compact(1, None).await.unwrap_err();
    assert_eq!(status_code(&error), Code::Unimplemented);
}

fn status_code(error: &Error) -> Code {
    match error {
        Error::GRpcStatus(status) => status.code(),
        other => panic!("expected a gRPC status, got {other:?}"),
    }
}
