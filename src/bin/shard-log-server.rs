use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use shard_log::{
    DurableLokiConfig, DurableLokiStore, LokiApiConfig, NativeServerConfig, StripeConfig,
    loki_router, loki_router_with_clickhouse, serve_native,
};

#[derive(Debug, Parser)]
#[command(
    name = "shard-log-server",
    about = "Standalone Loki-compatible ShardLog server"
)]
struct Arguments {
    /// HTTP listen address.
    #[arg(long, default_value = "0.0.0.0:3100")]
    listen: SocketAddr,
    /// High-throughput native binary protocol listen address.
    #[arg(long, default_value = "0.0.0.0:3101")]
    native_listen: SocketAddr,
    /// Tenant used when X-Scope-OrgID is absent.
    #[arg(long, default_value = "fake")]
    default_tenant: String,
    /// Maximum number of entries returned by one query.
    #[arg(long, default_value_t = 5_000)]
    max_query_limit: usize,
    /// Durable local data directory.
    #[arg(long, default_value = "./shard-log-data")]
    data_directory: PathBuf,
    /// Optional local directory used as the object-store backend.
    #[arg(long)]
    object_store_directory: Option<PathBuf>,
    /// Retain a duplicate raw-payload journal to accelerate hot-index recovery.
    #[arg(long, default_value_t = false)]
    recovery_journal: bool,
    /// Number of physical owner stripes.
    #[arg(long, default_value_t = 16)]
    shards: u32,
    /// Stable tenant partitions spread over physical stripes.
    #[arg(long, default_value_t = 256)]
    tenant_partitions: u32,
    /// Microseconds shard-stream may collect adjacent appends before one sync.
    #[arg(long, default_value_t = 250)]
    append_linger_micros: u64,
    /// Maximum seconds a durable append waits for indexed read visibility.
    #[arg(long, default_value_t = 300)]
    indexed_ack_timeout_seconds: u64,
    /// Acknowledge native appends after the compressed WAL commit instead of
    /// waiting for the stripe index to become query-visible.
    #[arg(long, default_value_t = false)]
    native_durable_ack: bool,
    /// File containing the bearer token that enables the ClickHouse scan route.
    /// When omitted, the route is not registered.
    #[arg(long)]
    clickhouse_token_file: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let arguments = Arguments::parse();
    if arguments.max_query_limit == 0 {
        return Err("--max-query-limit must be nonzero".into());
    }
    let listener = tokio::net::TcpListener::bind(arguments.listen).await?;
    let native_listener = tokio::net::TcpListener::bind(arguments.native_listen).await?;
    let store = Arc::new(DurableLokiStore::open(DurableLokiConfig {
        data_directory: arguments.data_directory,
        object_store_directory: arguments.object_store_directory,
        recovery_journal: arguments.recovery_journal,
        shard_count: arguments.shards,
        tenant_partitions: arguments.tenant_partitions,
        append_linger: std::time::Duration::from_micros(arguments.append_linger_micros),
        stripe: StripeConfig::default(),
        indexed_ack_timeout: std::time::Duration::from_secs(arguments.indexed_ack_timeout_seconds),
    })?);
    let api_config = LokiApiConfig {
        default_tenant: Arc::from(arguments.default_tenant),
        max_query_limit: arguments.max_query_limit,
    };
    let app = match arguments.clickhouse_token_file {
        Some(path) => {
            let token = std::fs::read_to_string(&path)?;
            let token = token.trim();
            if token.is_empty() {
                return Err(format!("ClickHouse token file {} is empty", path.display()).into());
            }
            loki_router_with_clickhouse(store.clone(), api_config, Arc::from(token))?
        }
        None => loki_router(store.clone(), api_config),
    };
    let (shutdown, _) = tokio::sync::broadcast::channel::<()>(1);
    let signal = shutdown.clone();
    tokio::spawn(async move {
        shutdown_signal().await;
        let _ = signal.send(());
    });
    let mut http_shutdown = shutdown.subscribe();
    let mut native_shutdown = shutdown.subscribe();
    let http = axum::serve(listener, app).with_graceful_shutdown(async move {
        let _ = http_shutdown.recv().await;
    });
    let native = serve_native(
        native_listener,
        store,
        NativeServerConfig {
            wait_for_index: !arguments.native_durable_ack,
            ..NativeServerConfig::default()
        },
        async move {
            let _ = native_shutdown.recv().await;
        },
    );
    tokio::try_join!(http, native)?;
    Ok(())
}

async fn shutdown_signal() {
    let interrupt = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = interrupt => {}
        () = terminate => {}
    }
}
