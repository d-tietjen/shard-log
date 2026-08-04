use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::RwLock;

use serde::{Deserialize, Serialize};

use crate::LokiApiError;

const DELETE_CATALOG_VERSION: u32 = 1;

/// One durable Loki-compatible logical deletion request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteRequest {
    /// Stable monotonically allocated hexadecimal request ID.
    pub request_id: String,
    /// Inclusive lower timestamp bound in Unix nanoseconds.
    pub start_time: i64,
    /// Inclusive upper timestamp bound in Unix nanoseconds.
    pub end_time: i64,
    /// Validated LogQL stream selector and line-filter expression.
    pub query: String,
    /// Lifecycle state exposed through the Loki API.
    pub status: String,
    /// Request creation time in Unix nanoseconds.
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DeleteState {
    next_request_id: u64,
    tenants: BTreeMap<String, Vec<DeleteRequest>>,
}

impl Default for DeleteState {
    fn default() -> Self {
        Self {
            next_request_id: 1,
            tenants: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct PersistedDeleteCatalog {
    version: u32,
    state: DeleteState,
    checksum: String,
}

/// Bounded mutation point for logical deletion state.
#[derive(Debug)]
pub(crate) struct DeleteCatalog {
    path: Option<PathBuf>,
    state: RwLock<DeleteState>,
}

impl Default for DeleteCatalog {
    fn default() -> Self {
        Self::memory()
    }
}

impl DeleteCatalog {
    pub(crate) fn memory() -> Self {
        Self {
            path: None,
            state: RwLock::new(DeleteState::default()),
        }
    }

    pub(crate) fn open(path: PathBuf) -> Result<Self, LokiApiError> {
        let state = if path.exists() {
            load_state(&path)?
        } else {
            DeleteState::default()
        };
        Ok(Self {
            path: Some(path),
            state: RwLock::new(state),
        })
    }

    pub(crate) fn create(
        &self,
        tenant: &str,
        start_time: i64,
        end_time: i64,
        query: String,
        created_at: i64,
    ) -> Result<String, LokiApiError> {
        if start_time > end_time {
            return Err(LokiApiError::bad_request(
                "delete start timestamp must not exceed end timestamp",
            ));
        }
        let mut current = self
            .state
            .write()
            .map_err(|_| LokiApiError::internal("delete catalog lock is poisoned"))?;
        let mut next = current.clone();
        let request_id = format!("{:016x}", next.next_request_id);
        next.next_request_id = next
            .next_request_id
            .checked_add(1)
            .ok_or_else(|| LokiApiError::internal("delete request ID space exhausted"))?;
        next.tenants
            .entry(tenant.to_owned())
            .or_default()
            .push(DeleteRequest {
                request_id: request_id.clone(),
                start_time,
                end_time,
                query,
                status: "received".to_owned(),
                created_at,
            });
        self.persist(&next)?;
        *current = next;
        Ok(request_id)
    }

    pub(crate) fn list(&self, tenant: &str) -> Result<Vec<DeleteRequest>, LokiApiError> {
        let state = self
            .state
            .read()
            .map_err(|_| LokiApiError::internal("delete catalog lock is poisoned"))?;
        Ok(state.tenants.get(tenant).cloned().unwrap_or_default())
    }

    pub(crate) fn cancel(&self, tenant: &str, request_id: &str) -> Result<bool, LokiApiError> {
        let mut current = self
            .state
            .write()
            .map_err(|_| LokiApiError::internal("delete catalog lock is poisoned"))?;
        let mut next = current.clone();
        let Some(requests) = next.tenants.get_mut(tenant) else {
            return Ok(false);
        };
        let previous_len = requests.len();
        requests.retain(|request| request.request_id != request_id);
        if requests.len() == previous_len {
            return Ok(false);
        }
        if requests.is_empty() {
            next.tenants.remove(tenant);
        }
        self.persist(&next)?;
        *current = next;
        Ok(true)
    }

    pub(crate) fn replace_tenant(
        &self,
        tenant: &str,
        mut requests: Vec<DeleteRequest>,
    ) -> Result<(), LokiApiError> {
        if tenant.is_empty() {
            return Err(LokiApiError::bad_request("delete tenant must not be empty"));
        }
        requests.sort_unstable_by(|left, right| left.request_id.cmp(&right.request_id));
        if requests.iter().any(|request| {
            request.request_id.is_empty()
                || request.start_time > request.end_time
                || !matches!(request.status.as_str(), "received" | "canceled")
        }) || requests
            .windows(2)
            .any(|pair| pair[0].request_id == pair[1].request_id)
        {
            return Err(LokiApiError::bad_request(
                "replicated delete requests are invalid or duplicated",
            ));
        }
        let next_request_id = requests
            .iter()
            .filter_map(|request| u64::from_str_radix(&request.request_id, 16).ok())
            .max()
            .and_then(|maximum| maximum.checked_add(1))
            .unwrap_or(1);
        let mut current = self
            .state
            .write()
            .map_err(|_| LokiApiError::internal("delete catalog lock is poisoned"))?;
        let mut next = current.clone();
        next.next_request_id = next.next_request_id.max(next_request_id);
        if requests.is_empty() {
            next.tenants.remove(tenant);
        } else {
            next.tenants.insert(tenant.to_owned(), requests);
        }
        self.persist(&next)?;
        *current = next;
        Ok(())
    }

    fn persist(&self, state: &DeleteState) -> Result<(), LokiApiError> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        let parent = path.parent().ok_or_else(|| {
            LokiApiError::configuration("delete catalog path must have a parent directory")
        })?;
        fs::create_dir_all(parent).map_err(delete_io_error)?;
        let state_bytes = serde_json::to_vec(state)
            .map_err(|error| LokiApiError::internal(format!("encode delete catalog: {error}")))?;
        let persisted = PersistedDeleteCatalog {
            version: DELETE_CATALOG_VERSION,
            state: state.clone(),
            checksum: blake3::hash(&state_bytes).to_hex().to_string(),
        };
        let encoded = serde_json::to_vec(&persisted)
            .map_err(|error| LokiApiError::internal(format!("encode delete catalog: {error}")))?;
        let temporary = temporary_path(path);
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)
            .map_err(delete_io_error)?;
        file.write_all(&encoded).map_err(delete_io_error)?;
        file.sync_all().map_err(delete_io_error)?;
        fs::rename(&temporary, path).map_err(delete_io_error)?;
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(delete_io_error)?;
        Ok(())
    }
}

fn load_state(path: &Path) -> Result<DeleteState, LokiApiError> {
    let encoded = fs::read(path).map_err(delete_io_error)?;
    let persisted: PersistedDeleteCatalog = serde_json::from_slice(&encoded)
        .map_err(|error| LokiApiError::internal(format!("decode delete catalog: {error}")))?;
    if persisted.version != DELETE_CATALOG_VERSION {
        return Err(LokiApiError::internal(format!(
            "unsupported delete catalog version {}",
            persisted.version
        )));
    }
    let state_bytes = serde_json::to_vec(&persisted.state)
        .map_err(|error| LokiApiError::internal(format!("encode delete catalog: {error}")))?;
    let observed = blake3::hash(&state_bytes).to_hex().to_string();
    if observed != persisted.checksum {
        return Err(LokiApiError::internal(
            "delete catalog checksum does not match its state",
        ));
    }
    if persisted.state.next_request_id == 0 {
        return Err(LokiApiError::internal(
            "delete catalog next request ID must be nonzero",
        ));
    }
    Ok(persisted.state)
}

fn temporary_path(path: &Path) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(".tmp");
    path.with_file_name(name)
}

fn delete_io_error(error: std::io::Error) -> LokiApiError {
    LokiApiError::internal(format!("delete catalog I/O failed: {error}"))
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    #[test]
    fn durable_catalog_survives_restart_and_allocates_monotonic_ids() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "shard-telemetry-deletes-{}-{nonce}",
            std::process::id()
        ));
        let path = directory.join("deletes.json");
        let catalog = DeleteCatalog::open(path.clone()).expect("catalog");
        assert_eq!(
            catalog
                .create("tenant-a", 1, 2, "{app=\"api\"}".to_owned(), 3)
                .expect("create"),
            "0000000000000001"
        );
        drop(catalog);

        let catalog = DeleteCatalog::open(path).expect("recover");
        assert_eq!(catalog.list("tenant-a").expect("list").len(), 1);
        assert_eq!(
            catalog
                .create("tenant-a", 4, 5, "{app=\"worker\"}".to_owned(), 6)
                .expect("create"),
            "0000000000000002"
        );
        assert!(
            catalog
                .cancel("tenant-a", "0000000000000001")
                .expect("cancel")
        );
        assert_eq!(catalog.list("tenant-a").expect("list").len(), 1);
        fs::remove_dir_all(directory).expect("cleanup");
    }

    #[test]
    fn corrupt_catalog_fails_closed() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "shard-telemetry-delete-corrupt-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&directory).expect("directory");
        let path = directory.join("deletes.json");
        fs::write(&path, b"{\"version\":1}").expect("corrupt catalog");
        assert!(DeleteCatalog::open(path).is_err());
        fs::remove_dir_all(directory).expect("cleanup");
    }
}
