# Descriptor (catalog-info.yaml)

## General Configurations

Every workload is defined by a set of specifications.

* `ID: [String]*` the unique identifier of the Workload component. This will never change in the life of a Data Product
  or the component itself.
  Constraints:
    * allowed characters are `[a-zA-Z0-9]` and `[_-]`.
    * the ID is a URN of the form `urn:dmb:cmp:$DPDomain:$DPName:$DPMajorVersion:$WorkloadName`.
* `Name: [String]*` the name of the Workload. This name is used also for display purposes, so it can contain all kind of
  characters. When used inside the Workload ID all special characters are replaced with standard ones and spaces are
  replaced with dashes.
* `FullyQualifiedName: [Optional[String]]` human-readable name that describes better the Workload.
* `Description: [String]` detailed explanation about the purpose of the workload, what sources are reading, what
  business logic is applying, etc.
* `Kind: [String]*` type of the entity. Since this is a Workload the only allowed value is `workload`.
* `Version: [String]*` specific version of the workload. Displayed as `X.Y.Z` where X is the major version of the Data
  Product, Y is the minor feature and Z is the patch. Major version (X) is also shown in the component ID and those
  fields (version and ID) are always aligned with one another. Please note that the major version of the component *must
  always* correspond to the major version of the Data Product it belongs to.
  Constraints:
    * Major version of the Data Product is always the same as the major version of all of its components, and it is the
      same version that is shown in both Data Product ID and component ID.
* `InfrastructureTemplateId: [String]*` the id of the microservice responsible for provisioning the component. A
  microservice may be capable of provisioning several components generated from different use case templates.
* `UseCaseTemplateId: [Option[String]]*` the id of the template used in the builder to create the component. Could be
  empty in case the component was not created from a builder template.
* `DependsOn: [Array[String]]*` A component could depend on other components belonging to the same Data Product, for
  example a SQL Output Port could be dependent on a Raw Output Port because it is just an external table. This is also
  used to define the provisioning order among components.
  Constraints:
    * This array will only contain IDs of other components of the same Data Product.
* `Platform: [Option[String]]` represents the vendor: Azure, GCP, AWS, CDP on AWS, etc. It is a free field, but it is
  useful to understand better the platform where the component will be running.
* `Technology: [Option[String]]` represents which technology is used to define the workload, like: Spark, Flink,
  pySpark, etc. The underlying technology is useful to understand better how the workload process data.
* `WorkloadType: [Option[String]]` explains what type of workload is: Ingestion ETL, Streaming, Internal Process, etc.
* `ConnectionType: [Option[String]]` an enum with allowed values: `[HouseKeeping|DataPipeline]`; `Housekeeping` is for
  all the workloads that are acting on internal data without any external dependency. `DataPipeline` instead is for
  workloads that are reading from output of other DP or external systems.
* `Tags: [Array[Yaml]]` Tag labels at Workload level (please refer
  to [OpenMetadata](https://docs.open-metadata.org/v1.1.0/main-concepts/metadata-standard/schemas/type/taglabel)).
* `ReadsFrom: [Array[String]]` This is filled only for `DataPipeline` workloads, and it represents the list of Output
  Ports or external systems that the workload uses as input. Output Ports are identified with `DP_UK:$OutputPortName`,
  while external systems will be defined by a URN in the form `urn:dmb:ex:$SystemName`. This filed can be elaborated
  more in the future and create a more semantic struct.
  Constraints:
    * This array will only contain Output Port IDs and/or external systems identifiers.
* `Specific: [Yaml]` this is a custom section where we can put all the information strictly related to a specific
  technology or dependent from a standard/policy defined in the federated governance.

## Specific Configurations

In this section custom configurations to provision a Flink workload component will be described.

* `artifact [Yaml]` Refer to the [Artifact section](#artifact)
* `job [Yaml]` Refer to the [Job section](#job)
* `clusterId [Yaml]` The component id of the related Flink Cluster

### Artifact

Here you specify the coordinates of your artifact on Artifactory. These will be used by the provisioner to fetch the
artifact and configure the workflow template.

* `name [String]` it is the name of the artifact (e.g. my-awesome-flink-job)
* `version [String]` it is the version of the artifact (e.g. 1.0.0)

### Job

This is the configuration of the Dataproc Job.

* `type [String]` it is used for internal purposes, should be `FLINK`
* `mainClass [String]` it is the main class of your code
* `args [Array[String]]` list of arguments that are passed to the job when the workflow template is instantiated.
* `savepointsPath [String]` possible values: "", auto or path to GCS folder: the path to the savepoints from which the
  user wishes to run the job. Please refer to [Savepoints Behavior page](savepoints_behaviour.md) for more details.

> IMPORTANT: the folder with the savepoints can be found in the bucket provided as the dependency for the Dataproc
> cluster,
> in the folder of the Data Product major version, going to the "savepoints/name_of_the_workload/dp_major"
> e.g gs://dp_name/dp_major/savepoints/workload_name/dp_major/
