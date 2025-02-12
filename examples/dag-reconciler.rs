use std::sync::Arc;

use k8s_openapi::api::core::v1::{
    ConfigMap, ConfigMapVolumeSource, Container, Pod, PodSpec, Volume, VolumeMount,
};
use kube::{
    api::{DeleteParams, ObjectMeta, PartialObjectMetaExt, Patch, PatchParams},
    Api, Client,
};
use simple_kube_controller::{
    dag::{DagBuilder, DagReconciler, TaskStatus},
    Config, Controller, ControllerConfig,
};
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
    let client = Client::try_default().await?;
    let ctx = Arc::new(Context {
        client: client.clone(),
    });
    let api = Api::default_namespaced(client);
    let on_create_or_update = DagBuilder::new()
        .start_with([Task::AddAnnotation, Task::ListConfigMapFiles])
        .then([Task::DeletePod])
        .build();
    let on_delete = DagBuilder::new()
        .start_with([Task::RemoveAnnotation])
        .then([Task::RemoveFinalizer])
        .build();
    let rec = DagReconciler::new()
        .on_create_or_update(on_create_or_update)
        .on_delete(on_delete);
    let cfg = Config {
        controller: ControllerConfig {
            finalizer: Some(FINALIZER.into()),
            ..Default::default()
        },
        ..Default::default()
    };
    let ctrl = Controller::start(api, rec, ctx, cfg);
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

const ANNOT_KEY: &str = "my-annot";
const ANNOT_VALUE: &str = "true";
const APP_NAME: &str = "my-app";
const FINALIZER: &str = "therealm.tech/finalizer";
const POD_NAME: &str = "my-pod";

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
    #[error("unnamed pod")]
    Unnamed,
    #[error("unnamespaced pod")]
    Unnamespaced,
}

struct Context {
    client: Client,
}

enum Task {
    AddAnnotation,
    DeletePod,
    ListConfigMapFiles,
    RemoveAnnotation,
    RemoveFinalizer,
}

impl Task {
    async fn add_annotation(res: Arc<ConfigMap>, ctx: Arc<Context>) -> Result<TaskStatus, Error> {
        let cm_name = res.metadata.name.as_ref().ok_or(Error::Unnamed)?;
        let ns = res.metadata.namespace.as_ref().ok_or(Error::Unnamespaced)?;
        let api = Api::<ConfigMap>::namespaced(ctx.client.clone(), ns);
        let mut annots = res.metadata.annotations.clone().unwrap_or_default();
        annots.insert(ANNOT_KEY.into(), ANNOT_VALUE.into());
        let meta = ObjectMeta {
            annotations: Some(annots),
            ..Default::default()
        };
        let partial = meta.into_request_partial::<ConfigMap>();
        let params = PatchParams::apply(APP_NAME);
        let patch = Patch::Apply(&partial);
        api.patch_metadata(cm_name, &params, &patch).await?;
        info!("annotation `{ANNOT_KEY}={ANNOT_VALUE}` added on configmap `{cm_name}`");
        Ok(TaskStatus::Completed)
    }

    async fn delete_pod(res: Arc<ConfigMap>, ctx: Arc<Context>) -> Result<TaskStatus, Error> {
        let ns = res.metadata.namespace.as_ref().ok_or(Error::Unnamespaced)?;
        let api = Api::<Pod>::namespaced(ctx.client.clone(), ns);
        let pod = api.get_opt(POD_NAME).await?;
        if pod.is_some() {
            let params = DeleteParams::default();
            api.delete(POD_NAME, &params).await?;
            info!("pod `{POD_NAME}` deleted");
        }
        Ok(TaskStatus::Completed)
    }

    async fn list_configmap_files(
        res: Arc<ConfigMap>,
        ctx: Arc<Context>,
    ) -> Result<TaskStatus, Error> {
        let cm_name = res.metadata.name.as_ref().ok_or(Error::Unnamed)?;
        let ns = res.metadata.namespace.as_ref().ok_or(Error::Unnamespaced)?;
        let api = Api::<Pod>::namespaced(ctx.client.clone(), ns);
        let pod = api.get_opt(POD_NAME).await?;
        if let Some(pod) = pod {
            if let Some(status) = pod.status {
                if let Some(phase) = status.phase {
                    if phase == "Succeeded" {
                        info!("pod `{POD_NAME}` is completed");
                        Ok(TaskStatus::Completed)
                    } else {
                        Ok(TaskStatus::InProgress)
                    }
                } else {
                    Ok(TaskStatus::InProgress)
                }
            } else {
                Ok(TaskStatus::InProgress)
            }
        } else {
            let pod = Pod {
                metadata: ObjectMeta {
                    name: Some(POD_NAME.into()),
                    namespace: Some(ns.into()),
                    ..Default::default()
                },
                spec: Some(PodSpec {
                    containers: vec![Container {
                        args: Some(vec![
                            "bash".into(),
                            "-c".into(),
                            "ls /config ; sleep 30".into(),
                        ]),
                        image: Some("bash".into()),
                        name: "bash".into(),
                        volume_mounts: Some(vec![VolumeMount {
                            mount_path: "/config".into(),
                            name: "config".into(),
                            read_only: Some(true),
                            ..Default::default()
                        }]),
                        ..Default::default()
                    }],
                    restart_policy: Some("Never".into()),
                    volumes: Some(vec![Volume {
                        name: "config".into(),
                        config_map: Some(ConfigMapVolumeSource {
                            name: cm_name.into(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }]),
                    ..Default::default()
                }),
                ..Default::default()
            };
            let params = PatchParams::apply(APP_NAME);
            let patch = Patch::Apply(&pod);
            api.patch(POD_NAME, &params, &patch).await?;
            info!("pod `{POD_NAME}` started");
            Ok(TaskStatus::InProgress)
        }
    }

    async fn remove_annotation(
        res: Arc<ConfigMap>,
        ctx: Arc<Context>,
    ) -> Result<TaskStatus, Error> {
        let cm_name = res.metadata.name.as_ref().ok_or(Error::Unnamed)?;
        let ns = res.metadata.namespace.as_ref().ok_or(Error::Unnamespaced)?;
        let api = Api::<ConfigMap>::namespaced(ctx.client.clone(), ns);
        let mut annots = res.metadata.annotations.clone().unwrap_or_default();
        annots.remove(ANNOT_KEY);
        let meta = ObjectMeta {
            annotations: Some(annots),
            ..Default::default()
        };
        let partial = meta.into_request_partial::<ConfigMap>();
        let params = PatchParams::apply(APP_NAME);
        let patch = Patch::Apply(&partial);
        api.patch_metadata(cm_name, &params, &patch).await?;
        info!("annotation `{ANNOT_KEY}={ANNOT_VALUE}` removed from configmap `{cm_name}`");
        Ok(TaskStatus::Completed)
    }

    async fn remove_finalizer(res: Arc<ConfigMap>, ctx: Arc<Context>) -> Result<TaskStatus, Error> {
        let cm_name = res.metadata.name.as_ref().ok_or(Error::Unnamed)?;
        let ns = res.metadata.namespace.as_ref().ok_or(Error::Unnamespaced)?;
        let api = Api::<ConfigMap>::namespaced(ctx.client.clone(), ns);
        let mut finalizers = res.metadata.finalizers.clone().unwrap_or_default();
        finalizers.retain(|finalizer| finalizer != FINALIZER);
        let meta = ObjectMeta {
            finalizers: Some(finalizers),
            ..Default::default()
        };
        let partial = meta.into_request_partial::<ConfigMap>();
        let params = PatchParams::apply(APP_NAME);
        let patch = Patch::Apply(&partial);
        api.patch_metadata(cm_name, &params, &patch).await?;
        info!("finalizer `{FINALIZER}` removed from configmap `{cm_name}`");
        Ok(TaskStatus::Completed)
    }
}

impl simple_kube_controller::dag::Task<Context, Error, ConfigMap> for Task {
    fn name(&self) -> &str {
        match self {
            Self::AddAnnotation => "add_annotation",
            Self::DeletePod => "delete_pod",
            Self::ListConfigMapFiles => "list_configmap_files",
            Self::RemoveAnnotation => "remove_annotation",
            Self::RemoveFinalizer => "remove_finalizer",
        }
    }

    async fn run(&self, res: Arc<ConfigMap>, ctx: Arc<Context>) -> Result<TaskStatus, Error> {
        match self {
            Self::AddAnnotation => Self::add_annotation(res, ctx).await,
            Self::DeletePod => Self::delete_pod(res, ctx).await,
            Self::ListConfigMapFiles => Self::list_configmap_files(res, ctx).await,
            Self::RemoveAnnotation => Self::remove_annotation(res, ctx).await,
            Self::RemoveFinalizer => Self::remove_finalizer(res, ctx).await,
        }
    }
}
