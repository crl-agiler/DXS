# Glossary

This document lists key terms referenced throughout the documentation.

* `Artifact` - archived version of the application. In case of Flink it's a `.jar` file with all the included packages
  and files.
* `Cluster` - is a set of computing resources that you can use to run Apache Spark and Apache Hadoop jobs. The cluster
  includes both worker nodes (for distributed processing) and master nodes (for coordination).
* `Dataproc` - Google Cloud service to run multiple types of the
  application. [Official doc](https://cloud.google.com/dataproc?hl=en)
* `Flink App` - a Flink application written in Java.
* `Job` - a Flink application that a Data Product team deploys on the Dataproc Cluster
* `Service account` - a technical user with special abilities that is used to initiate
  cluster. [Official doc](https://cloud.google.com/iam/docs/service-account-overview)
* `Savepoints` - is a consistent image of the execution state of a streaming job, created via Flink’s checkpointing
  mechanism, [Official doc](https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/ops/state/savepoints/)
* `Yarn` - which stands for "Yet Another Resource Negotiator," is a cluster management technology that is part of the
  Apache Hadoop project. It was introduced in Hadoop version 2.0 to overcome the limitations of the original MapReduce
  framework, and it allows for the efficient management of resources in large clusters and the execution of distributed
  applications.
