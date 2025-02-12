# simple-kube-controller

[![doc](https://docs.rs/simple-kube-controller/badge.svg)](https://docs.rs/simple-kube-controller/latest/simple_kube_controller/)
![quality](https://github.com/therealm-tech/simple-kube-controller/actions/workflows/quality.yml/badge.svg)
![release](https://github.com/therealm-tech/simple-kube-controller/actions/workflows/release.yml/badge.svg)

The simple-kube-controller library streamlines the process of developing custom Kubernetes controllers.

## Getting started

To begin using simple-kube-controller, add the following dependency to your `Cargo.toml` file:
```toml
simple-kube-controller = "0"
```

## Overview

Kubernetes controllers are responsible for monitoring resource events such as creation, updates, and deletions. The simple-kube-controller leverages a reconciler to handle these events efficiently.

For a practical demonstration, refer to the [custom reconciler example](examples/custom-reconciler.rs).

### DAG reconciler

By default, the `dag` feature is enabled, allowing you to utilize the `DagReconciler`. A Directed Acyclic Graph (DAG) reconciler organizes tasks into groups that are executed asynchronously upon receiving an event. Each group of tasks is initiated only after the previous group has successfully completed, ensuring a sequential and controlled execution flow.

For more details, check out the [DAG reconciler example](examples/dag-reconciler.rs).

## Build

To build the project, run the following command:
```bash
cargo build
```

## Test

To execute tests, use the command:
```bash
cargo test
```
