use std::{fmt::Debug, future::Future, marker::PhantomData, sync::Arc, time::Duration};

use kube::{runtime::controller::Action, Resource};
use serde::de::DeserializeOwned;
use tokio::task::JoinSet;
use tracing::{debug, error, instrument};

use crate::{record_resource_metadata, Reconciler};

/// A task status.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskStatus {
    /// The task is completed.
    Completed,
    /// The task is still in progress.
    InProgress,
}

/// A task.
pub trait Task<
    CONTEXT: Send + Sync,
    ERROR: std::error::Error + Send + Sync,
    RESOURCE: Clone + Debug + DeserializeOwned + Resource<DynamicType = ()> + Send + Sync,
>: Send + Sync
{
    /// Name of the task.
    fn name(&self) -> &str;

    /// Run the task.
    /// Arguments:
    ///   - `res`: the resource
    ///   - `ctx`: the context of the application
    ///
    /// It should return [`TaskStatus::Completed`] if the task is done and [`TaskStatus::InProgress`] if it is not.
    fn run(
        &self,
        res: Arc<RESOURCE>,
        ctx: Arc<CONTEXT>,
    ) -> impl Future<Output = Result<TaskStatus, ERROR>> + Send;
}

/// A DAG.
pub struct Dag<
    CONTEXT: Send + Sync,
    ERROR: std::error::Error + Send + Sync,
    RESOURCE: Clone + Debug + DeserializeOwned + Resource<DynamicType = ()> + Send + Sync,
    TASK: Task<CONTEXT, ERROR, RESOURCE>,
> {
    action: Action,
    requeue_delay: Duration,
    tasks: Vec<Vec<Arc<TASK>>>,
    _ctx: PhantomData<CONTEXT>,
    _err: PhantomData<ERROR>,
    _res: PhantomData<RESOURCE>,
}

impl<
        CONTEXT: Send + Sync + 'static,
        ERROR: std::error::Error + Send + Sync + 'static,
        RESOURCE: Clone + Debug + DeserializeOwned + Resource<DynamicType = ()> + Send + Sync + 'static,
        TASK: Task<CONTEXT, ERROR, RESOURCE> + 'static,
    > Dag<CONTEXT, ERROR, RESOURCE, TASK>
{
    /// Run a DAG.
    /// Arguments:
    ///   - `res`: the resource
    ///   - `ctx`: the context of the application
    #[instrument(
        fields(
            resource.api_version = %RESOURCE::api_version(&()),
            resource.name,
            resource.namespace,
        ),
        skip(self, res, ctx)
    )]
    pub async fn run(&self, res: Arc<RESOURCE>, ctx: Arc<CONTEXT>) -> Result<Action, ERROR> {
        record_resource_metadata!(res.meta());
        let mut idx = 0;
        while idx < self.tasks.len() {
            let mut completed = 0;
            let tasks = &self.tasks[idx];
            let mut handles = JoinSet::new();
            for task in tasks {
                let task = task.clone();
                let res = res.clone();
                let ctx = ctx.clone();
                let name = task.name().to_string();
                debug!("starting task `{name}`");
                handles.spawn(async move { task.run(res, ctx).await.map(|status| (name, status)) });
            }
            while let Some(res) = handles.join_next().await {
                match res {
                    Ok(Ok((name, TaskStatus::Completed))) => {
                        debug!("task `{name}` completed");
                        completed += 1;
                    }
                    Ok(Ok((task, TaskStatus::InProgress))) => {
                        debug!("task `{task}` still in progress");
                    }
                    Ok(Err(err)) => return Err(err),
                    Err(err) => {
                        error!("failed to wait for task run: {err}");
                    }
                }
            }
            if completed == tasks.len() {
                idx += 1;
            } else {
                break;
            }
        }
        if idx == self.tasks.len() {
            Ok(self.action.clone())
        } else {
            Ok(Action::requeue(self.requeue_delay))
        }
    }
}

impl<
        CONTEXT: Send + Sync,
        ERROR: std::error::Error + Send + Sync,
        RESOURCE: Clone + Debug + DeserializeOwned + Resource<DynamicType = ()> + Send + Sync,
        TASK: Task<CONTEXT, ERROR, RESOURCE>,
    > Default for Dag<CONTEXT, ERROR, RESOURCE, TASK>
{
    fn default() -> Self {
        Self {
            action: Action::await_change(),
            requeue_delay: Duration::from_secs(15),
            tasks: vec![],
            _ctx: PhantomData,
            _err: PhantomData,
            _res: PhantomData,
        }
    }
}

/// A DAG builder.
#[derive(Default)]
pub struct DagBuilder<
    CONTEXT: Send + Sync,
    ERROR: std::error::Error + Send + Sync,
    RESOURCE: Clone + Debug + DeserializeOwned + Resource<DynamicType = ()> + Send + Sync,
    TASK: Task<CONTEXT, ERROR, RESOURCE>,
>(Dag<CONTEXT, ERROR, RESOURCE, TASK>);

impl<
        CONTEXT: Send + Sync,
        ERROR: std::error::Error + Send + Sync,
        RESOURCE: Clone + Debug + DeserializeOwned + Resource<DynamicType = ()> + Send + Sync,
        TASK: Task<CONTEXT, ERROR, RESOURCE>,
    > DagBuilder<CONTEXT, ERROR, RESOURCE, TASK>
{
    /// Create a DAG builder.
    pub fn new() -> Self {
        Self(Default::default())
    }

    /// Define the final action of the DAG (when all tasks are completed).
    pub fn action(mut self, action: Action) -> Self {
        self.0.action = action;
        self
    }

    /// Define the delay between two reconciliations.
    pub fn requeue_delay(mut self, delay: Duration) -> Self {
        self.0.requeue_delay = delay;
        self
    }

    /// Define the first group tasks to run. The tasks will be run asynchronously.
    pub fn start_with<TASKS: IntoIterator<Item = TASK>>(
        mut self,
        tasks: TASKS,
    ) -> DagBuilderThen<CONTEXT, ERROR, RESOURCE, TASK> {
        self.0.tasks = vec![tasks.into_iter().map(Arc::new).collect()];
        DagBuilderThen(self.0)
    }
}

/// A DAG builder with the first group of tasks already configured.
pub struct DagBuilderThen<
    CONTEXT: Send + Sync,
    ERROR: std::error::Error + Send + Sync,
    RESOURCE: Clone + Debug + DeserializeOwned + Resource<DynamicType = ()> + Send + Sync,
    TASK: Task<CONTEXT, ERROR, RESOURCE>,
>(Dag<CONTEXT, ERROR, RESOURCE, TASK>);

impl<
        CONTEXT: Send + Sync,
        ERROR: std::error::Error + Send + Sync,
        RESOURCE: Clone + Debug + DeserializeOwned + Resource<DynamicType = ()> + Send + Sync,
        TASK: Task<CONTEXT, ERROR, RESOURCE>,
    > DagBuilderThen<CONTEXT, ERROR, RESOURCE, TASK>
{
    /// Define the final action of the DAG (when all tasks are completed).
    pub fn action(mut self, action: Action) -> Self {
        self.0.action = action;
        self
    }

    /// Build the DAG.
    pub fn build(self) -> Dag<CONTEXT, ERROR, RESOURCE, TASK> {
        self.0
    }

    /// Define the delay between two reconciliations.
    pub fn requeue_delay(mut self, delay: Duration) -> Self {
        self.0.requeue_delay = delay;
        self
    }

    /// Define the next group of tasks to run. It will be started when all tasks of the current group are completed.
    pub fn then<TASKS: IntoIterator<Item = TASK>>(mut self, tasks: TASKS) -> Self {
        self.0.tasks.push(tasks.into_iter().map(Arc::new).collect());
        self
    }
}

/// A DAG reconciler.
#[derive(Default)]
pub struct DagReconciler<
    CONTEXT: Send + Sync,
    ERROR: std::error::Error + Send + Sync,
    RESOURCE: Clone + Debug + DeserializeOwned + Resource<DynamicType = ()> + Send + Sync,
    TASK: Task<CONTEXT, ERROR, RESOURCE>,
> {
    on_create_or_update: Option<Dag<CONTEXT, ERROR, RESOURCE, TASK>>,
    on_delete: Option<Dag<CONTEXT, ERROR, RESOURCE, TASK>>,
}

impl<
        CONTEXT: Send + Sync + 'static,
        ERROR: std::error::Error + Send + Sync + 'static,
        RESOURCE: Clone + Debug + DeserializeOwned + Resource<DynamicType = ()> + Send + Sync + 'static,
        TASK: Task<CONTEXT, ERROR, RESOURCE> + 'static,
    > DagReconciler<CONTEXT, ERROR, RESOURCE, TASK>
{
    /// Create a new DAG reconciler.
    pub fn new() -> Self {
        Self {
            on_create_or_update: None,
            on_delete: None,
        }
    }

    /// Define the DAG to run when a resource is created/updated.
    pub fn on_create_or_update(mut self, dag: Dag<CONTEXT, ERROR, RESOURCE, TASK>) -> Self {
        self.on_create_or_update = Some(dag);
        self
    }

    /// Define the DAG to run when a resource is deleted.
    pub fn on_delete(mut self, dag: Dag<CONTEXT, ERROR, RESOURCE, TASK>) -> Self {
        self.on_delete = Some(dag);
        self
    }
}

impl<
        CONTEXT: Send + Sync + 'static,
        ERROR: std::error::Error + Send + Sync + 'static,
        RESOURCE: Clone + Debug + DeserializeOwned + Resource<DynamicType = ()> + Send + Sync + 'static,
        TASK: Task<CONTEXT, ERROR, RESOURCE> + 'static,
    > Reconciler<CONTEXT, ERROR, RESOURCE> for DagReconciler<CONTEXT, ERROR, RESOURCE, TASK>
{
    #[instrument(
        fields(
            resource.api_version = %RESOURCE::api_version(&()),
            resource.name,
            resource.namespace,
        ),
        skip(self, res, ctx)
    )]
    async fn reconcile_creation_or_update(
        &self,
        res: Arc<RESOURCE>,
        ctx: Arc<CONTEXT>,
    ) -> Result<Action, ERROR> {
        record_resource_metadata!(res.meta());
        if let Some(dag) = &self.on_create_or_update {
            dag.run(res, ctx).await
        } else {
            Ok(Action::await_change())
        }
    }

    #[instrument(
        fields(
            resource.api_version = %RESOURCE::api_version(&()),
            resource.name,
            resource.namespace,
        ),
        skip(self, res, ctx)
    )]
    async fn reconcile_deletion(
        &self,
        res: Arc<RESOURCE>,
        ctx: Arc<CONTEXT>,
    ) -> Result<Action, ERROR> {
        record_resource_metadata!(res.meta());
        if let Some(dag) = &self.on_delete {
            dag.run(res, ctx).await
        } else {
            Ok(Action::await_change())
        }
    }
}

#[cfg(test)]
mod test {
    use std::convert::Infallible;

    use k8s_openapi::api::core::v1::Namespace;

    use super::*;

    struct DummyTask(TaskStatus);

    impl Task<(), Infallible, Namespace> for DummyTask {
        fn name(&self) -> &str {
            "dummy"
        }

        async fn run(&self, _res: Arc<Namespace>, _ctx: Arc<()>) -> Result<TaskStatus, Infallible> {
            Ok(self.0)
        }
    }

    mod dag {
        use super::*;

        mod run {
            use super::*;

            #[tokio::test]
            async fn when_first_group_in_progress() {
                let delay = Duration::from_secs(10);
                let dag = DagBuilder::new()
                    .requeue_delay(delay)
                    .start_with([
                        DummyTask(TaskStatus::InProgress),
                        DummyTask(TaskStatus::Completed),
                    ])
                    .then([DummyTask(TaskStatus::InProgress)])
                    .build();
                let action = dag
                    .run(Arc::new(Namespace::default()), Arc::new(()))
                    .await
                    .unwrap();
                assert_eq!(action, Action::requeue(delay));
            }

            #[tokio::test]
            async fn when_second_group_in_progress() {
                let delay = Duration::from_secs(10);
                let dag = DagBuilder::new()
                    .requeue_delay(delay)
                    .start_with([
                        DummyTask(TaskStatus::Completed),
                        DummyTask(TaskStatus::Completed),
                    ])
                    .then([DummyTask(TaskStatus::InProgress)])
                    .build();
                let action = dag
                    .run(Arc::new(Namespace::default()), Arc::new(()))
                    .await
                    .unwrap();
                assert_eq!(action, Action::requeue(delay));
            }

            #[tokio::test]
            async fn when_completed() {
                let exepcted = Action::await_change();
                let dag = DagBuilder::new()
                    .action(exepcted.clone())
                    .start_with([
                        DummyTask(TaskStatus::Completed),
                        DummyTask(TaskStatus::Completed),
                    ])
                    .then([DummyTask(TaskStatus::Completed)])
                    .build();
                let action = dag
                    .run(Arc::new(Namespace::default()), Arc::new(()))
                    .await
                    .unwrap();
                assert_eq!(action, exepcted);
            }
        }
    }
}
