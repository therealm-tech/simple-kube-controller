use std::{fmt::Debug, future::Future, marker::PhantomData, sync::Arc, time::Duration};

use futures::StreamExt;
use kube::{
    api::{ObjectMeta, PartialObjectMetaExt, Patch, PatchParams},
    runtime::{controller::Action, reflector::ObjectRef},
    Api, Resource,
};
use serde::de::DeserializeOwned;
use tokio::{
    spawn,
    sync::oneshot::Sender,
    task::{JoinError, JoinHandle},
};
use tracing::{debug, error, instrument};

/// DAG-oriented reconciler.
#[cfg(feature = "dag")]
pub mod dag;

macro_rules! record_resource_metadata {
    ($meta:expr) => {{
        let span = tracing::Span::current();
        if let Some(name) = &$meta.name {
            span.record("resource.name", name);
        }
        if let Some(ns) = &$meta.namespace {
            span.record("resource.namespace", ns);
        }
    }};
}
pub(crate) use record_resource_metadata;

/// Error when the controller is stopped.
#[derive(Debug, thiserror::Error)]
pub enum StopError {
    #[error("failed to wait for controller task: {0}")]
    Join(
        #[from]
        #[source]
        JoinError,
    ),
    #[error("failed to send stop signal")]
    Send,
}

/// A configuration.
#[derive(Clone, Default, Debug)]
pub struct Config {
    /// The controller configuration.
    pub controller: ControllerConfig,
    /// The watcher configuration.
    pub watcher: kube::runtime::watcher::Config,
}

/// A controller configuration.
#[derive(Clone, Debug)]
pub struct ControllerConfig {
    // If defined, add automatically finalizer if it is not present on the resource.
    pub finalizer: Option<String>,
    /// Action to do when an error occurred. By default, the event is requeued for 10s.
    pub on_error: Action,
}

impl Default for ControllerConfig {
    fn default() -> Self {
        Self {
            finalizer: None,
            on_error: Action::requeue(Duration::from_secs(10)),
        }
    }
}

/// A reconciler.
pub trait Reconciler<
    CONTEXT: Send + Sync,
    ERROR: std::error::Error + Send + Sync,
    RESOURCE: Clone + Debug + DeserializeOwned + Resource<DynamicType = ()> + Send + Sync,
>: Send + Sync
{
    /// Handle a resource creation/update.
    /// Arguments:
    ///   - `res`: the resource
    ///   - `ctx`: the context of the application
    fn reconcile_creation_or_update(
        &self,
        res: Arc<RESOURCE>,
        ctx: Arc<CONTEXT>,
    ) -> impl Future<Output = Result<Action, ERROR>> + Send;

    /// Handle a resource deletion.
    /// Arguments:
    ///   - `res`: the resource
    ///   - `ctx`: the context of the application
    fn reconcile_deletion(
        &self,
        res: Arc<RESOURCE>,
        ctx: Arc<CONTEXT>,
    ) -> impl Future<Output = Result<Action, ERROR>> + Send;
}

/// A controller.
pub struct Controller<
    RESOURCE: Clone + Debug + DeserializeOwned + Resource<DynamicType = ()> + Send + Sync,
> {
    handle: JoinHandle<()>,
    stop_tx: Sender<()>,
    _res: PhantomData<RESOURCE>,
}

impl<
        RESOURCE: Clone + Debug + DeserializeOwned + Resource<DynamicType = ()> + Send + Sync + 'static,
    > Controller<RESOURCE>
{
    /// Start a controller.
    /// Arguments:
    ///   - `api`: the API object
    ///   - `rec`: the reconciler to use
    ///   - `ctx`: the context to give to the reconciler
    ///   - `cfg`: the configuration (see [`Config`])
    ///
    /// The controller is started in background.
    #[instrument(
        fields(
            resource.api_version = %RESOURCE::api_version(&()),
        ),
        skip(api, rec, ctx, cfg)
    )]
    pub fn start<
        CONTEXT: Send + Sync + 'static,
        ERROR: std::error::Error + Send + Sync + 'static,
        RECONCILER: Reconciler<CONTEXT, ERROR, RESOURCE> + 'static,
    >(
        api: Api<RESOURCE>,
        rec: RECONCILER,
        ctx: Arc<CONTEXT>,
        cfg: Config,
    ) -> Self {
        let ctx = Arc::new(Context::new(rec, ctx, cfg.controller, api.clone()));
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        let ctrl = kube::runtime::Controller::new(api, cfg.watcher)
            .graceful_shutdown_on(async move {
                if let Err(err) = stop_rx.await {
                    error!("failed to receive stop signal: {err}");
                }
                debug!("stop signal received");
            })
            .run(Self::reconcile, Self::on_error, ctx)
            .for_each(Self::on_each);
        debug!("controller started");
        Self {
            handle: spawn(ctrl),
            stop_tx,
            _res: PhantomData,
        }
    }

    /// Stop the controller.
    #[instrument(
        fields(
            resource.api_version = %RESOURCE::api_version(&()),
        ),
        skip(self),
    )]
    pub async fn stop(self) -> Result<(), StopError> {
        debug!("stopping signal");
        self.stop_tx.send(()).map_err(|_| StopError::Send)?;
        debug!("waiting for graceful shutdown");
        self.handle.await?;
        debug!("controller stopped");
        Ok(())
    }

    #[instrument(
        parent = None,
        fields(
            resource.api_version = %RESOURCE::api_version(&()),
            resource.name,
            resource.namespace,
        ),
        skip(res)
    )]
    async fn on_each<ERROR: std::error::Error + Send + Sync + 'static>(
        res: Result<
            (ObjectRef<RESOURCE>, Action),
            kube::runtime::controller::Error<ReconcileError<ERROR>, kube::runtime::watcher::Error>,
        >,
    ) {
        match res {
            Ok((res, _)) => {
                let span = tracing::Span::current();
                span.record("resource.name", res.name);
                if let Some(ns) = &res.namespace {
                    span.record("resource.namespace", ns);
                }
                debug!("resource reconciled");
            }
            Err(err) => error!("{err}"),
        }
    }

    #[instrument(
        parent = None,
        fields(
            resource.api_version = %RESOURCE::api_version(&()),
            resource.name,
            resource.namespace,
        ),
        skip(res, ctx)
    )]
    fn on_error<
        CONTEXT: Send + Sync,
        ERROR: std::error::Error + Send + Sync,
        RECONCILER: Reconciler<CONTEXT, ERROR, RESOURCE>,
    >(
        res: Arc<RESOURCE>,
        err: &ReconcileError<ERROR>,
        ctx: Arc<Context<CONTEXT, ERROR, RECONCILER, RESOURCE>>,
    ) -> Action {
        record_resource_metadata!(res.meta());
        ctx.config.on_error.clone()
    }

    #[instrument(
        parent = None,
        fields(
            resource.api_version = %RESOURCE::api_version(&()),
            resource.name,
            resource.namespace,
        ),
        skip(res, ctx)
    )]
    async fn reconcile<
        CONTEXT: Send + Sync,
        ERROR: std::error::Error + Send + Sync,
        RECONCILER: Reconciler<CONTEXT, ERROR, RESOURCE>,
    >(
        res: Arc<RESOURCE>,
        ctx: Arc<Context<CONTEXT, ERROR, RECONCILER, RESOURCE>>,
    ) -> Result<Action, ReconcileError<ERROR>> {
        let meta = res.meta();
        record_resource_metadata!(meta);
        if meta.deletion_timestamp.is_some() {
            debug!("resource has been deleted");
            ctx.reconciler
                .reconcile_deletion(res, ctx.global.clone())
                .await
                .map_err(ReconcileError::App)
        } else {
            debug!("resource has been created or updated");
            if let Some(finalizer) = &ctx.config.finalizer {
                let mut finalizers = meta.finalizers.clone().unwrap_or_default();
                if finalizers.contains(finalizer) {
                    debug!("finalizers already contain `{finalizer}`");
                    ctx.reconciler
                        .reconcile_creation_or_update(res, ctx.global.clone())
                        .await
                        .map_err(ReconcileError::App)
                } else {
                    let name = meta.name.as_ref().ok_or(ReconcileError::ResourceUnnamed)?;
                    finalizers.push(finalizer.clone());
                    let meta = ObjectMeta {
                        finalizers: Some(finalizers),
                        ..Default::default()
                    };
                    let partial = meta.into_request_partial::<RESOURCE>();
                    let params = PatchParams::apply(env!("CARGO_PKG_NAME"));
                    let patch = Patch::Apply(&partial);
                    debug!("adding finalizer `{finalizer}` on resource `{name}`");
                    ctx.api
                        .patch_metadata(name, &params, &patch)
                        .await
                        .map_err(ReconcileError::Kube)?;
                    Ok(Action::requeue(Duration::from_secs(0)))
                }
            } else {
                ctx.reconciler
                    .reconcile_creation_or_update(res, ctx.global.clone())
                    .await
                    .map_err(ReconcileError::App)
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
enum ReconcileError<ERROR: std::error::Error + Send + Sync> {
    App(#[source] ERROR),
    Kube(#[source] kube::Error),
    #[error("resource is unnamed")]
    ResourceUnnamed,
}

struct Context<
    CONTEXT: Send + Sync,
    ERROR: std::error::Error + Send + Sync,
    RECONCILER: Reconciler<CONTEXT, ERROR, RESOURCE>,
    RESOURCE: Clone + Debug + DeserializeOwned + Resource<DynamicType = ()> + Send + Sync,
> {
    api: Api<RESOURCE>,
    config: ControllerConfig,
    global: Arc<CONTEXT>,
    reconciler: RECONCILER,
    _err: PhantomData<ERROR>,
}

impl<
        CONTEXT: Send + Sync,
        ERROR: std::error::Error + Send + Sync,
        RECONCILER: Reconciler<CONTEXT, ERROR, RESOURCE>,
        RESOURCE: Clone + Debug + DeserializeOwned + Resource<DynamicType = ()> + Send + Sync,
    > Context<CONTEXT, ERROR, RECONCILER, RESOURCE>
{
    fn new(rec: RECONCILER, ctx: Arc<CONTEXT>, cfg: ControllerConfig, api: Api<RESOURCE>) -> Self {
        Self {
            api,
            config: cfg,
            global: ctx,
            reconciler: rec,
            _err: PhantomData,
        }
    }
}
