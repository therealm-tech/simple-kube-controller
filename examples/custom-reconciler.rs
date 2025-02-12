use std::{sync::Arc, time::Duration};

use k8s_openapi::api::core::v1::Namespace;
use kube::{runtime::controller::Action, Api};
use simple_kube_controller::{Controller, Reconciler};
use tokio::{
    select,
    signal::unix::{signal, SignalKind},
};
use tracing::{debug, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(EnvFilter::new("debug"))
        .init();
    let mut sigint = signal(SignalKind::interrupt())?;
    let mut sigterm = signal(SignalKind::terminate())?;
    let ctx = Arc::new(Context {
        delay: Duration::from_secs(10),
    });
    let client = kube::Client::try_default().await?;
    let api = Api::all(client);
    let ctrl = Controller::start(api, NamespaceReconciler, ctx, Default::default());
    select! {
        _ = sigint.recv() => {
            debug!("sigint received");
        }
        _ = sigterm.recv() => {
            debug!("sigterm received");
        }
    }
    ctrl.stop().await?;
    Ok(())
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
enum Error {
    Io(
        #[from]
        #[source]
        std::io::Error,
    ),
    Kube(
        #[from]
        #[source]
        kube::Error,
    ),
    Stop(
        #[from]
        #[source]
        simple_kube_controller::StopError,
    ),
}

struct Context {
    delay: Duration,
}

struct NamespaceReconciler;

impl Reconciler<Context, Error, Namespace> for NamespaceReconciler {
    async fn reconcile_creation_or_update(
        &self,
        _res: Arc<Namespace>,
        ctx: Arc<Context>,
    ) -> Result<Action, Error> {
        info!("new namespace created or updated!");
        Ok(Action::requeue(ctx.delay))
    }

    async fn reconcile_deletion(
        &self,
        _res: Arc<Namespace>,
        _ctx: Arc<Context>,
    ) -> Result<Action, Error> {
        info!("namespace deleted!");
        Ok(Action::await_change())
    }
}
