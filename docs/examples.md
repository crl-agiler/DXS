# Examples

## Example 1: Default example

```yaml
  artifact:
    name: flink-artifact
    version: 1.0.0
  job:
    type: FLINK
    mainClass: eu.unicredit.document.dxstraceinfo.App
    args: []
    savepointsPath: ""
  clusterId: custerId
```

## Example 2: Force automatic savepoint behavior

```yaml
  artifact:
    name: flink-artifact
    version: 1.0.0
  job:
    type: FLINK
    mainClass: eu.unicredit.document.dxstraceinfo.App
    args: []
    savepointsPath: auto
  clusterId: custerId
```

## Example 3: Specify savepoints on GCS

```yaml
  artifact:
    name: flink-artifact
    version: 1.0.0
  job:
    type: FLINK
    mainClass: eu.unicredit.document.dxstraceinfo.App
    args: []
    savepointsPath: gs://dp_name/dp_major/savepoints/workload_name/dp_major/
  clusterId: custerId
```
