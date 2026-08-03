# Repository Structure

This section outlines the structure of the folder created in BitBucket after component creation. It also highlights the
main properties of its elements.

## Structure

<pre>
mesh_your_component/
│
├── devops/
│   ├── configuration.yaml
│   ├── application.yaml
│
├── docs/
│
├── environments/
│   ├── development/
│       ├── configurations.yaml
│   ├── quality/
│       ├── configurations.yaml
│   ├── production/
│       ├── configurations.yaml
│
├── flink-app/
│   ├── distribution/
│       ├── configs/
│   ├── src/
│       ├── main/
│           ├── eu.unicredit.examples/
│               ├── App.java
│   ├── pom.xml
│
├── catalog-info.yaml
│
└── ...
</pre>

## `catalog-info.yaml`

This file is the input for the Witboost Provisioner. It describes the Data Mesh component and its specifications. Learn
more about catalog-info in [Descriptor (catalog-info.yaml)](descriptor.md).

## `flink-app/`

This is where your application starts. Here you define everything.

- `src/` - is where the application is located, here in `main/` is defined the application and in `test/` are defined
  the tests
  > NOTE: You can change the name of the main class, by changing it in `catalog-info.yaml` field `mainClass`
- `distribution/` - This folder, alongside Python files, is uploaded to the GCS bucket.
    - `configs/` - Add necessary items here, but remember, it's the only folder excluded entirely from the Workflow
      Template. To reference files, provide the full GCS path.
      Example: gs:
      //bucket_name/dp_major_version/storage_area_name/flink_component_name/version/distribution/configs/development/.donotremove

- `pom.xml` - a fundamental component of a Maven-based Java project. Feel free to add any dependencies you need, but be
  cautions in deleting the dependencies that are already included, since they might be crucial for the Flink App

## `environments/`

Files here override fields in `catalog-info.yaml` based on the environment. For instance, to set different service
accounts for various environments, provide the path and new value in the `configuration.yaml` of the respective folder.

```yaml
specific:
  job:
    args:
      - --bucket=my_bucket_name
  cluster:
    serviceAccount: my service account.
```

## `devops/`

Defines parameters for the Jenkins pipeline. Avoid changing it.
