---
title: Config
description: Configuring the Graft SQLite extension
---

The SQLite extension can be configured using either a configuration file (`graft.toml`) or environment variables.

The extension searches for the configuration file in the current directory or in the following standard locations:

| Platform      | Configuration Path                  | Example                                           |
| ------------- | ----------------------------------- | ------------------------------------------------- |
| Linux & macOS | `$XDG_CONFIG_HOME/graft/graft.toml` | `/home/alice/.config/graft/graft.toml`            |
| Windows       | `%APPDATA%\graft\graft.toml`        | `C:\Users\Alice\AppData\Roaming\graft\graft.toml` |

If the `GRAFT_CONFIG` environment variable is set it will be used instead of searching.

## Configuration Options

### `data_dir`

- **Environment variable:** `GRAFT_DIR`
- **Description:** Path to the directory where Graft stores its local data (Fjall LSM storage).
- **Default:**
  - Linux & macOS: `$XDG_DATA_HOME/graft` or `~/.local/share/graft`
  - Windows: `%LOCALAPPDATA%\graft` or `C:\Users\%USERNAME%\AppData\Local\graft`

### `remote`

Configuration for remote object storage. This is where Graft stores the source of truth for your data.

#### `remote.type = "memory"`

In-memory object storage. Useful for testing and development.

```toml
[remote]
type = "memory"
```

#### `remote.type = "fs"`

Local filesystem storage. Good for development and single-machine deployments.

```toml
[remote]
type = "fs"
root = "/path/to/storage"
```

- **`root`**: Path to the directory where remote data is stored.

#### `remote.type = "s3_compatible"`

S3-compatible object storage (AWS S3, MinIO, R2, etc.). Recommended for production.

```toml
[remote]
type = "s3_compatible"
bucket = "my-graft-bucket"
prefix = "optional/prefix"  # optional
```

- **`bucket`**: S3 bucket name.
- **`prefix`**: Optional path prefix within the bucket.

**Credentials:** S3 credentials and configuration are loaded from standard AWS environment variables:

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_REGION`
- `AWS_ENDPOINT` (for S3-compatible services like MinIO, R2, etc.)

#### `remote.type = "gcs"`

Native Google Cloud Storage, using Google's object APIs rather than the S3 interoperability endpoint.

```toml
[remote]
type = "gcs"
bucket = "my-graft-bucket"
prefix = "optional/prefix"  # optional
```

- **`bucket`**: An existing GCS bucket name, without the `gs://` prefix. Graft does not create buckets.
- **`prefix`**: Optional object-name prefix within the bucket. All clients sharing a remote must use the same bucket and prefix.

The equivalent environment variables are `GRAFT_REMOTE__TYPE=gcs`, `GRAFT_REMOTE__BUCKET`, and `GRAFT_REMOTE__PREFIX`.

**Credentials:** The native OpenDAL GCS backend loads service-account, impersonated-service-account, or external-account credentials from `GOOGLE_APPLICATION_CREDENTIALS` or its well-known application-credentials location. On Google Cloud, including Cloud Run, it can obtain short-lived access tokens from the metadata server using the attached service account. Prefer that keyless service identity in production; no AWS HMAC keys are required.

The currently pinned credential loader does **not** support plain `authorized_user` credential files produced by a normal `gcloud auth application-default login`. Use a supported service-account or external-account credential file for local testing instead of assuming every ADC credential type is supported.

The identity needs permission to read and create objects. Streaming segment uploads also use GCS multipart-upload operations; `roles/storage.objectUser` supplies the object and multipart permissions. The live qualification test additionally lists and deletes its own test objects. Grant access only to the intended bucket or managed folder, and do not configure lifecycle rules that delete live Graft logs or segments.

**Write safety:** Commit publication uses `ifGenerationMatch=0`, so concurrent writers cannot overwrite the commit at an occupied log position. Commits remain single conditional uploads, including records larger than the default upload chunk size: the pinned client's XML multipart path does not support this precondition. A commit larger than the backend's maximum single chunk is rejected before upload. Segments can still use streaming multipart uploads. An uncertain commit response is reconciled by Graft's existing commit-recovery path, not by an unconditional overwrite.

Keep `data_dir` on local disk. It is a cache, not the durable remote. Do not configure a Cloud Storage FUSE mount as `remote.type = "fs"` or as the local cache; filesystem emulation is not a substitute for GCS generation preconditions.

**Qualification:** Automated tests exercise the native HTTP client against a local, stateful GCS protocol fixture. A separate ignored test checks real GCS commit collisions, large conditional commits, segment ranges, multipart uploads, and recovery with empty local storage:

```bash
GRAFT_GCS_TEST_BUCKET=disposable-test-bucket \
  cargo test -p graft gcs_live_bucket_qualification -- --ignored --nocapture
```

This test requires Google credentials, creates a random `graft-qualification/<id>/` prefix, and deletes that prefix after successful completion. For a one-off local run where the available ADC file is an unsupported credential type, `GRAFT_GCS_TEST_TOKEN` can supply a short-lived OAuth access token to the test harness only; it is not a production Graft configuration option. It does not run in ordinary test suites. A failed run can leave objects under the printed prefix; clean up only that test prefix. Passing the local fixture tests alone does not qualify a production bucket or deployment.

### `autosync`

- **Environment variable:** `GRAFT_AUTOSYNC`
- **Description:** Background synchronization interval in seconds. When set, Graft will automatically sync volumes with the remote at this interval.
- **Default:** Not set (no automatic synchronization)
- **Example:** `autosync = 60` (sync every 60 seconds)

### `log_file`

- **Environment variable:** `GRAFT_LOG_FILE`
- **Description:** Write a verbose log of all Graft operations to the specified log file. Verbosity can be controlled using the `RUST_LOG` environment variable.
- **Valid verbosity levels:** `error`, `warn`, `info`, `debug`, `trace`

### `make_default`

- **Environment variable:** `GRAFT_MAKE_DEFAULT`
- **Description:** Cause the Graft VFS to become the default VFS for all new database connections.

## Example Configurations

### Production (S3)

```toml
data_dir = "/var/lib/graft"
autosync = 60

[remote]
type = "s3_compatible"
bucket = "my-app-graft"
prefix = "prod"
```

Set environment variables:

```bash
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_REGION="us-east-1"
```

### Google Cloud Storage (Cloud Run)

```toml
data_dir = "/tmp/graft-cache"

[remote]
type = "gcs"
bucket = "my-app-graft"
prefix = "prod"
```

Attach a service account with the bucket permissions described above to the Cloud Run service. Each instance uses its own disposable local cache while sharing the remote bucket and prefix. A successful local SQLite transaction is not itself a remote-durability acknowledgement; explicitly push before acknowledging a write that must survive instance termination.

### Development (Filesystem)

```toml
data_dir = "./data"

[remote]
type = "fs"
root = "./remote-storage"
```

### Testing (In-Memory)

```toml
[remote]
type = "memory"
```
