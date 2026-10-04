use std::{collections::HashMap, sync::Arc};

use axum::{
    Router,
    body::Body,
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use bilrost::Message;
use bytes::Bytes;
use futures::TryStreamExt;
use parking_lot::Mutex;
use serde::Deserialize;
use serde_json::json;
use tokio::{net::TcpListener, task::JoinHandle};

use super::{Remote, RemoteConfig, build_cloud_operator};
use crate::{
    core::{LogId, PageCount, PageIdx, SegmentId, commit::Commit, lsn::LSN, page::Page},
    local::fjall_storage::FjallStorage,
    rt::runtime::Runtime,
    volume_reader::VolumeRead,
    volume_writer::VolumeWrite,
};

// Exercise the native OpenDAL client over HTTP. The server owns real object state and
// enforces GCS generation preconditions rather than returning scripted SDK results.
#[derive(Default)]
struct GcsObjects {
    objects: HashMap<String, Bytes>,
    conditional_uploads: usize,
    fail_next_create_response: bool,
    deny_uploads: bool,
}

struct GcsTestServer {
    endpoint: String,
    objects: Arc<Mutex<GcsObjects>>,
    task: JoinHandle<()>,
}

impl GcsTestServer {
    async fn start() -> Self {
        setup_gcs_test_faults();
        let objects = Arc::new(Mutex::new(GcsObjects::default()));
        let app = Router::new()
            .route("/upload/storage/v1/b/{bucket}/o", post(upload_object))
            .route("/storage/v1/b/{bucket}/o/{*object}", get(read_object))
            .layer(DefaultBodyLimit::max(32 * 1024 * 1024))
            .with_state(objects.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self { endpoint, objects, task }
    }

    fn remote(&self, prefix: &str) -> Remote {
        let builder = opendal::services::Gcs::default()
            .bucket("graft-test")
            .root(prefix)
            .endpoint(&self.endpoint)
            .disable_config_load()
            .disable_vm_metadata()
            .token("graft-test-token".to_string());
        Remote {
            store: build_cloud_operator(builder).unwrap(),
        }
    }
}

impl Drop for GcsTestServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[derive(Deserialize)]
struct UploadQuery {
    name: String,
    #[serde(rename = "uploadType")]
    upload_type: String,
    #[serde(rename = "ifGenerationMatch")]
    if_generation_match: Option<u64>,
}

async fn upload_object(
    State(objects): State<Arc<Mutex<GcsObjects>>>,
    Path(bucket): Path<String>,
    Query(query): Query<UploadQuery>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    assert_eq!(bucket, "graft-test");
    assert_eq!(query.upload_type, "media");
    assert_eq!(headers[header::AUTHORIZATION], "Bearer graft-test-token");
    let mut objects = objects.lock();
    if objects.deny_uploads {
        return gcs_error(StatusCode::FORBIDDEN, "Permission denied");
    }
    if query.name.contains("/commits/") && query.if_generation_match != Some(0) {
        return gcs_error(
            StatusCode::BAD_REQUEST,
            "Commit upload lacks generation precondition",
        );
    }
    if let Some(generation) = query.if_generation_match {
        assert_eq!(generation, 0);
        objects.conditional_uploads += 1;
        if objects.objects.contains_key(&query.name) {
            return gcs_error(StatusCode::PRECONDITION_FAILED, "Object already exists");
        }
    }
    let size = body.len();
    objects.objects.insert(query.name.clone(), body);
    if query.if_generation_match.is_some() && std::mem::take(&mut objects.fail_next_create_response)
    {
        return gcs_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Upload committed; response failed",
        );
    }
    axum::Json(json!({
        "name": query.name,
        "size": size.to_string(),
        "etag": "test-etag",
        "generation": "1",
        "updated": "2026-01-01T00:00:00Z"
    }))
    .into_response()
}

async fn read_object(
    State(objects): State<Arc<Mutex<GcsObjects>>>,
    Path((bucket, object)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    assert_eq!(bucket, "graft-test");
    assert_eq!(headers[header::AUTHORIZATION], "Bearer graft-test-token");
    let Some(bytes) = objects.lock().objects.get(&object).cloned() else {
        return gcs_error(StatusCode::NOT_FOUND, "Object not found");
    };
    if let Some(range) = headers.get(header::RANGE) {
        let (start, end) = range
            .to_str()
            .unwrap()
            .strip_prefix("bytes=")
            .unwrap()
            .split_once('-')
            .unwrap();
        let start = start.parse::<usize>().unwrap();
        let end = end.parse::<usize>().unwrap();
        return Response::builder()
            .status(StatusCode::PARTIAL_CONTENT)
            .header(
                header::CONTENT_RANGE,
                format!("bytes {start}-{end}/{}", bytes.len()),
            )
            .body(Body::from(bytes.slice(start..=end)))
            .unwrap();
    }
    bytes.into_response()
}

fn gcs_error(status: StatusCode, message: &str) -> Response {
    (
        status,
        axum::Json(json!({ "error": { "code": status.as_u16(), "message": message } })),
    )
        .into_response()
}

#[test]
fn gcs_config_accepts_bucket_and_optional_prefix() {
    for (prefix_config, expected_prefix) in [
        ("", None),
        ("prefix = 'graft/production'", Some("graft/production")),
    ] {
        let config = config::Config::builder()
            .add_source(config::File::from_str(
                &format!("type = 'gcs'\nbucket = 'graft-test'\n{prefix_config}"),
                config::FileFormat::Toml,
            ))
            .build()
            .unwrap()
            .try_deserialize::<RemoteConfig>()
            .unwrap();
        let RemoteConfig::Gcs { bucket, prefix } = config else {
            panic!("expected native GCS config")
        };
        assert_eq!(bucket, "graft-test");
        assert_eq!(prefix.as_deref(), expected_prefix);
    }
}

#[test]
fn gcs_config_rejects_an_empty_bucket() {
    let config = RemoteConfig::Gcs { bucket: String::new(), prefix: None };
    let error = config.build().unwrap_err();
    assert!(
        matches!(error, super::RemoteErr::ObjectStore(error) if error.kind() == opendal::ErrorKind::ConfigInvalid)
    );
}

#[tokio::test]
async fn gcs_prefixes_isolate_log_positions_in_the_same_bucket() {
    let server = GcsTestServer::start().await;
    let first = server.remote("first");
    let second = server.remote("second");
    let log = LogId::random();
    let first_commit = Commit::new(log.clone(), LSN::FIRST, PageCount::ONE);
    let second_commit = Commit::new(log.clone(), LSN::FIRST, PageCount::new(2));
    first.put_commit(&first_commit).await.unwrap();
    second.put_commit(&second_commit).await.unwrap();
    assert_eq!(
        first.get_commit(&log, LSN::FIRST).await.unwrap(),
        Some(first_commit)
    );
    assert_eq!(
        second.get_commit(&log, LSN::FIRST).await.unwrap(),
        Some(second_commit)
    );
    assert_eq!(server.objects.lock().objects.len(), 2);
}

#[tokio::test]
async fn gcs_permission_failure_does_not_publish_a_commit() {
    let server = GcsTestServer::start().await;
    server.objects.lock().deny_uploads = true;
    let remote = server.remote("qualification");
    let log = LogId::random();
    let commit = Commit::new(log.clone(), LSN::FIRST, PageCount::ONE);
    let error = remote.put_commit(&commit).await.unwrap_err();
    assert!(
        matches!(error, super::RemoteErr::ObjectStore(error) if error.kind() == opendal::ErrorKind::PermissionDenied)
    );
    assert_eq!(remote.get_commit(&log, LSN::FIRST).await.unwrap(), None);
    assert!(server.objects.lock().objects.is_empty());
}

#[tokio::test]
async fn gcs_concurrent_commit_creators_cannot_overwrite_the_winner() {
    let server = GcsTestServer::start().await;
    assert_commit_collision(
        &server.remote("qualification"),
        &server.remote("qualification"),
    )
    .await;
    let objects = server.objects.lock();
    assert_eq!(objects.conditional_uploads, 2);
    assert_eq!(objects.objects.len(), 1);
    assert!(
        objects
            .objects
            .keys()
            .all(|key| key.starts_with("qualification/logs/"))
    );
}

async fn assert_commit_collision(first: &Remote, second: &Remote) {
    let log = LogId::random();
    let first_commit = Commit::new(log.clone(), LSN::FIRST, PageCount::ONE);
    let second_commit = Commit::new(log.clone(), LSN::FIRST, PageCount::new(2));
    let (first_result, second_result) = tokio::join!(
        first.put_commit(&first_commit),
        second.put_commit(&second_commit)
    );
    let winner = match (first_result, second_result) {
        (Ok(()), Err(error)) => {
            assert!(error.precondition_failed());
            first_commit
        }
        (Err(error), Ok(())) => {
            assert!(error.precondition_failed());
            second_commit
        }
        results => panic!("expected exactly one commit winner, got {results:?}"),
    };
    assert_eq!(
        second.get_commit(&log, LSN::FIRST).await.unwrap(),
        Some(winner)
    );
}

#[tokio::test]
async fn gcs_large_commit_preserves_generation_preconditions() {
    let server = GcsTestServer::start().await;
    assert_large_commit_collision(
        &server.remote("qualification"),
        &server.remote("qualification"),
    )
    .await;
}

async fn assert_large_commit_collision(source: &Remote, fresh: &Remote) {
    let log = LogId::random();
    let commit = Commit::new(log.clone(), LSN::new(3_000_001), PageCount::ONE)
        .with_checkpoints((1..=3_000_000).map(LSN::new).collect());
    assert!(commit.encode_to_bytes().len() > 5 * 1024 * 1024);
    source.put_commit(&commit).await.unwrap();
    let competing = Commit::new(log.clone(), commit.lsn(), PageCount::new(2));
    assert!(
        fresh
            .put_commit(&competing)
            .await
            .unwrap_err()
            .precondition_failed()
    );
    assert_eq!(
        fresh.get_commit(&log, commit.lsn()).await.unwrap(),
        Some(commit)
    );
}

#[tokio::test]
async fn gcs_commit_response_failure_does_not_overwrite_committed_data() {
    let server = GcsTestServer::start().await;
    server.objects.lock().fail_next_create_response = true;
    let remote = server.remote("qualification");
    let log = LogId::random();
    let original = Commit::new(log.clone(), LSN::FIRST, PageCount::ONE);
    // A transport retry after the failed response reaches the generation precondition.
    assert!(
        remote
            .put_commit(&original)
            .await
            .unwrap_err()
            .precondition_failed()
    );
    let competing = Commit::new(log.clone(), LSN::FIRST, PageCount::new(2));
    assert!(
        remote
            .put_commit(&competing)
            .await
            .unwrap_err()
            .precondition_failed()
    );
    assert_eq!(
        remote.get_commit(&log, LSN::FIRST).await.unwrap(),
        Some(original)
    );
    assert_eq!(server.objects.lock().objects.len(), 1);
}

#[tokio::test]
async fn gcs_reads_missing_commits_ordered_logs_and_segment_ranges() {
    let server = GcsTestServer::start().await;
    assert_remote_reads(
        &server.remote("qualification"),
        &server.remote("qualification"),
    )
    .await;
}

async fn assert_remote_reads(source: &Remote, fresh: &Remote) {
    let log = LogId::random();
    assert_eq!(fresh.get_commit(&log, LSN::FIRST).await.unwrap(), None);
    let commits = [
        Commit::new(log.clone(), LSN::FIRST, PageCount::ONE),
        Commit::new(log.clone(), LSN::new(2), PageCount::new(2)),
    ];
    for commit in &commits {
        source.put_commit(commit).await.unwrap();
    }
    let read: Vec<Commit> = fresh
        .stream_commits_ordered(&log, [LSN::FIRST, LSN::new(2), LSN::new(3)])
        .try_collect()
        .await
        .unwrap();
    assert_eq!(read, commits);
    let sid = SegmentId::random();
    source
        .put_segment(
            &sid,
            [Bytes::from_static(b"first-"), Bytes::from_static(b"second")],
        )
        .await
        .unwrap();
    assert_eq!(
        fresh.get_segment_range(&sid, 3..9).await.unwrap(),
        Bytes::from_static(b"st-sec")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gcs_runtime_restores_pushed_pages_with_empty_local_storage() {
    let server = GcsTestServer::start().await;
    assert_cache_recovery(
        server.remote("qualification"),
        server.remote("qualification"),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gcs_runtime_reconciles_a_committed_upload_after_response_failure() {
    let server = GcsTestServer::start().await;
    let source = server.remote("qualification");
    let fresh = server.remote("qualification");
    let objects = server.objects.clone();
    let handle = tokio::runtime::Handle::current();
    tokio::task::spawn_blocking(move || {
        let runtime = test_runtime(&handle, source);
        let volume = runtime.volume_open(None, None, None).unwrap();
        let writer = runtime.volume_writer(volume.vid.clone()).unwrap();
        write_test_page(writer);
        objects.lock().fail_next_create_response = true;
        runtime.volume_push(volume.vid.clone()).unwrap();
        restore_test_page(&handle, fresh, volume.remote);
    })
    .await
    .unwrap();
}

async fn assert_cache_recovery(source: Remote, fresh: Remote) {
    let handle = tokio::runtime::Handle::current();
    tokio::task::spawn_blocking(move || {
        let log = {
            let runtime = test_runtime(&handle, source);
            let volume = runtime.volume_open(None, None, None).unwrap();
            write_test_page(runtime.volume_writer(volume.vid.clone()).unwrap());
            runtime.volume_push(volume.vid).unwrap();
            volume.remote
        };
        // Both the local Fjall store and client are new; only remote objects survive.
        restore_test_page(&handle, fresh, log);
    })
    .await
    .unwrap();
}

fn setup_gcs_test_faults() {
    #[cfg(feature = "precept")]
    {
        // Workspace feature unification enables random crash faults. These scenarios
        // inject failures through HTTP instead, so random process exits would hide results.
        static INIT: std::sync::Once = std::sync::Once::new();
        INIT.call_once(|| {
            precept::init(&precept::dispatch::test::TestDispatch).unwrap();
            precept::fault::disable_all();
            crate::fault::set_crash_mode(true);
        });
    }
}

fn test_runtime(handle: &tokio::runtime::Handle, remote: Remote) -> Runtime {
    setup_gcs_test_faults();
    Runtime::new(
        handle.clone(),
        Arc::new(remote),
        Arc::new(FjallStorage::open_temporary().unwrap()),
        None,
    )
}

fn write_test_page(mut writer: crate::volume_writer::VolumeWriter) {
    writer
        .write_page(PageIdx::FIRST, Page::test_filled(123))
        .unwrap();
    writer.commit().unwrap();
}

fn restore_test_page(handle: &tokio::runtime::Handle, remote: Remote, log: LogId) {
    let restored = test_runtime(handle, remote);
    let volume = restored.volume_open(None, None, Some(log)).unwrap();
    restored.volume_pull(volume.vid.clone()).unwrap();
    assert_eq!(
        restored
            .volume_reader(volume.vid)
            .unwrap()
            .read_page(PageIdx::FIRST)
            .unwrap(),
        Page::test_filled(123)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires GRAFT_GCS_TEST_BUCKET and Google credentials; creates and deletes test objects"]
async fn gcs_live_bucket_qualification() {
    let bucket = std::env::var("GRAFT_GCS_TEST_BUCKET")
        .expect("set GRAFT_GCS_TEST_BUCKET to a disposable test bucket");
    let prefix = format!("graft-qualification/{}", LogId::random().serialize());
    eprintln!("GCS qualification prefix: {prefix}");
    let source = build_live_gcs_remote(&bucket, &prefix);
    let fresh = build_live_gcs_remote(&bucket, &prefix);
    assert_commit_collision(&source, &fresh).await;
    assert_remote_reads(&source, &fresh).await;
    assert_large_commit_collision(&source, &fresh).await;
    assert_cache_recovery(source.clone(), fresh.clone()).await;
    // Cross the native client's multipart threshold instead of only exercising small uploads.
    let sid = SegmentId::random();
    source
        .put_segment(
            &sid,
            [
                Bytes::from(vec![42; 8 * 1024 * 1024]),
                Bytes::from(vec![43; 8 * 1024 * 1024]),
            ],
        )
        .await
        .unwrap();
    let boundary = 8 * 1024 * 1024;
    assert_eq!(
        fresh
            .get_segment_range(&sid, boundary - 2..boundary + 2)
            .await
            .unwrap(),
        Bytes::from_static(&[42, 42, 43, 43])
    );
    source.store.delete_with("").recursive(true).await.unwrap();
}

fn build_live_gcs_remote(bucket: &str, prefix: &str) -> Remote {
    if let Some(token) = std::env::var("GRAFT_GCS_TEST_TOKEN")
        .ok()
        .filter(|token| !token.is_empty())
    {
        let builder = opendal::services::Gcs::default()
            .bucket(bucket)
            .root(prefix)
            .disable_config_load()
            .disable_vm_metadata()
            .token(token);
        return Remote {
            store: build_cloud_operator(builder).unwrap(),
        };
    }

    RemoteConfig::Gcs {
        bucket: bucket.to_string(),
        prefix: Some(prefix.to_string()),
    }
    .build()
    .unwrap()
}
