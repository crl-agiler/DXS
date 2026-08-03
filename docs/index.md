# Dataproc Flink Template

## Introduction

The Dataproc Flink use case template simplifies the deploying
of [Dataproc Flink](https://cloud.google.com/dataproc/docs/concepts/components/flink).

This templates will set up a dedicated repository in BitBucket to host your Flink application, as well as a readily
deployable descriptor that defines additional information (e.g. additional libraries, job arguments, etc.).

After deploying the component following the steps outlined in the [Flink Lifecycle](flink_lifecycle.md), a Flink will be
provisioned in the designated Google Cloud Project.

## Documentation Layout

- **[Flink Job Lifecycle](flink_lifecycle.md)**: explains the creation and deployment of the Flink component, revealing
  the underlying processes.
- **[Repository Structure](repository_structure.md)** : describes the repository that is created on BitBucket after the
  creation of the component
- **[Descriptor (catalog-info.yaml)](descriptor.md)**: in-depth description of each field found in the
  catalog-info.yaml.
- **[Savepoints Behaviour](savepoints_behaviour.md)**: description of how to assign and use savepoints for flink jobs.
- **[Examples](examples.md)**: Multiple instances showcasing the descriptor's application, including scenarios like file
  uploads and adjustments of cluster configuration.
- **[Tips](tips.md)**: Collection of valuable commands and recommendations.
- **[Glossary](glossary.md)**: Compilation of key terms referenced throughout the documentation.
- **[External links](external_links.md)**: Compilation of useful external links.

## Prerequisites

- #### Data Product

  Similar to other Witboost components, initiation begins with the creation of a Data Product.

- #### Flink Dataproc Cluster

  The Flink Dataproc Cluster component, where the job will be hosted.

- #### Service Account

  The Service Account functions as a technical user for operations on the Dataproc Cluster. The **Data Product team** is
  responsible for generating a service account for *each environment* (e.g., sit, uat, prod) for *each Data Product*.
  Detailed instructions for requesting service account creation are available in the following link:
  > See section *"Dataproc - Service Account dedicated to the Data product or Domain"* of
  the [Google Cloud Roles and permissions Guide](https://confluence.internal.unicredit.eu/display/AAHM4/Google+Cloud+Platform+Roles+and+permission)
