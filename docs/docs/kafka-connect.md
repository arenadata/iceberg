---
title: "Kafka Connect"
---
<!--
 - Licensed to the Apache Software Foundation (ASF) under one or more
 - contributor license agreements.  See the NOTICE file distributed with
 - this work for additional information regarding copyright ownership.
 - The ASF licenses this file to You under the Apache License, Version 2.0
 - (the "License"); you may not use this file except in compliance with
 - the License.  You may obtain a copy of the License at
 -
 -   http://www.apache.org/licenses/LICENSE-2.0
 -
 - Unless required by applicable law or agreed to in writing, software
 - distributed under the License is distributed on an "AS IS" BASIS,
 - WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 - See the License for the specific language governing permissions and
 - limitations under the License.
 -->

# Kafka Connect

[Kafka Connect](https://kafka.apache.org/documentation/#connect) is a popular framework for moving data
in and out of Apache Kafka via connectors. There are many different connectors available, such as the S3 sink
for writing data from Kafka to S3 and Debezium source connectors for writing change data capture records from relational
databases to Kafka.

It has a straightforward, decentralized, distributed architecture. A cluster consists of a number of worker processes,
and a connector runs tasks on these processes to perform the work. Connector deployment is configuration driven, so
generally no code needs to be written to run a connector.

## Apache Iceberg Sink Connector

The Apache Iceberg Sink Connector for Kafka Connect is a sink connector for writing data from Kafka into Iceberg tables.

## Features

* Commit coordination for centralized Iceberg commits
* Exactly-once delivery semantics
* Multi-table fan-out
* Change data capture
* Copy-on-write or merge-on-read for updates and deletes
* Automatic table creation and schema evolution
* Field name mapping via Iceberg’s column mapping functionality
* Optional NDV (number of distinct values) and partition statistics, updated on every commit

## Installation

The connector zip archive is created as part of the Iceberg build. You can run the build via:
```bash
./gradlew -x test -x integrationTest clean build
```
The zip archive will be found under `./kafka-connect/kafka-connect-runtime/build/distributions`. There is
one distribution that bundles the Hive Metastore client and related dependencies, and one that does not.
Copy the distribution archive into the Kafka Connect plugins directory on all nodes.

## Requirements

The sink relies on [KIP-447](https://cwiki.apache.org/confluence/display/KAFKA/KIP-447%3A+Producer+scalability+for+exactly+once+semantics)
for exactly-once semantics. This requires Kafka 2.5 or later.

Statistics [metrics](#metrics) need Kafka Connect 4.1 or later.

## Configuration

| Property                                                        | Description                                                                                                           |
|-----------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------|
| routing.strategy                                                | Record routing strategy: `all-tables`, `regex`, `dynamic-field`, or `topic-to-table`                                  |
| iceberg.tables                                                  | Comma-separated list of destination tables                                                                            |
| iceberg.tables.dynamic-enabled                                  | Set to `true` to route to a table specified in `routeField` instead of using `routeRegex`, default is `false`         |
| iceberg.tables.route-field                                      | For multi-table fan-out, the name of the field used to route records to tables                                        |
| iceberg.tables.topic-to-table-mapping                           | Comma-separated static mapping from Kafka topic names to Iceberg tables, for example `topic1:db.table1`               |
| iceberg.tables.topic-to-table-mapping-file                      | Absolute path to a JSON file with static mapping from Kafka topic names to Iceberg tables                             |
| iceberg.tables.default-commit-branch                            | Default branch for commits, main is used if not specified                                                             |
| iceberg.tables.default-id-columns                               | Default comma-separated list of columns that identify a row in tables (primary key)                                   |
| iceberg.tables.default-partition-by                             | Default comma-separated list of partition field names to use when creating tables                                     |
| iceberg.tables.auto-create-enabled                              | Set to `true` to automatically create destination tables, default is `false`                                          |
| iceberg.tables.evolve-schema-enabled                            | Set to `true` to add any missing record fields to the table schema, default is `false`                                |
| iceberg.tables.schema-force-optional                            | Set to `true` to set columns as optional during table create and evolution, default is `false` to respect schema      |
| iceberg.tables.schema-case-insensitive                          | Set to `true` to look up table columns by case-insensitive name, default is `false` for case-sensitive                |
| iceberg.tables.schema-timestamp-ns-fields                       | Comma-separated field names or exact field paths for Debezium `NanoTimestamp` fields to map to Iceberg `timestamp_ns` |
| iceberg.tables.cdc-field                                        | Source record field that identifies the type of operation (insert, update, or delete)                                 |
| iceberg.tables.cdc.ops.insert                                   | The comma-separated values of the cdc operation field corresponding to INSERT                                         |
| iceberg.tables.cdc.ops.update                                   | The comma-separated values of the cdc operation field corresponding to UPDATE                                         |
| iceberg.tables.cdc.ops.delete                                   | The comma-separated values of the cdc operation field corresponding to DELETE                                         |
| iceberg.tables.cdc.ops.ignored                                  | The comma-separated values of the cdc operation field that should be ignored by connector                             |
| iceberg.tables.upsert-mode-enabled                              | Set to true to treat all appends as upserts, false otherwise                                                          |
| iceberg.tables.row-level-mode                                   | How updates and deletes are applied: `merge-on-read` (default) or `copy-on-write`, for all tables of the connector    |
| iceberg.tables.staging-location                                 | Location of the connector's staging files (copy-on-write change files, statistics Puffin files), default is `<table location>/kc-staging` |
| iceberg.tables.copy-on-write.max-slice-keys                     | Max distinct keys applied by one commit, default is 200,000; a larger change set is applied over several commits      |
| iceberg.tables.copy-on-write.max-rewrite-bytes                  | Max total size of data files rewritten by one commit, default is 10 GiB                                               |
| iceberg.tables.copy-on-write.pruning.max-in-cardinality         | Max distinct values per identifier column in the file pruning predicate, default is 200                               |
| iceberg.tables.copy-on-write.commit-retries                     | Immediate retries of a failed or conflicting commit, default is 2; after that it is retried in the next cycle         |
| iceberg.tables.copy-on-write.rewrite-threads                    | Threads rewriting data files, per task, default is min(2, cores)                                                      |
| iceberg.tables.copy-on-write.rewrite-timeout-ms                 | Rewrite timeout in msec after which the work is retried, default is 120,000 (2 min)                                   |
| iceberg.tables.copy-on-write.rewrite-response-chunk-files       | Max data files reported in one control message, default is 500                                                        |
| iceberg.tables.staging-orphan-cleanup-interval-ms               | How often orphaned staged files are removed, default is 3,600,000 (1 hour)                                            |
| iceberg.tables.staging-orphan-ttl-ms                            | Age after which an unreferenced staged file is orphaned, default is 86,400,000 (1 day)                                |
| iceberg.tables.statistics.*                                     | Table statistics, off by default, see [Table statistics](#table-statistics)                                           |
| iceberg.tables.auto-create-props.*                              | Properties set on new tables during auto-create                                                                       |
| iceberg.tables.write-props.*                                    | Properties passed through to Iceberg writer initialization, these take precedence                                     |
| iceberg.table.\<table name\>.commit-branch                      | Table-specific branch for commits, use `iceberg.tables.default-commit-branch` if not specified                        |
| iceberg.table.\<table name\>.id-columns                         | Comma-separated list of columns that identify a row in the table (primary key)                                        |
| iceberg.table.\<table name\>.partition-by                       | Comma-separated list of partition fields to use when creating the table                                               |
| iceberg.table.\<table name\>.route-regex                        | The regex used to match a record's `routeField` to a table                                                            |
| iceberg.table.\<table name\>.statistics.*                       | Table-specific statistics settings, see [Table statistics](#table-statistics)                                         |
| iceberg.control.topic                                           | Name of the control topic, default is `control-iceberg`                                                               |
| iceberg.control.group-id-prefix                                 | Prefix for the control consumer group, default is `cg-control`                                                        |
| iceberg.control.commit.interval-ms                              | Commit interval in msec, default is 300,000 (5 min)                                                                   |
| iceberg.control.commit.timeout-ms                               | Commit timeout interval in msec, default is 30,000 (30 sec)                                                           |
| iceberg.control.commit.threads                                  | Number of threads to use for commits, default is (cores * 2)                                                          |
| iceberg.coordinator.transactional.prefix                        | Prefix for the transactional id to use for the coordinator producer, default is to use no/empty prefix                |
| iceberg.catalog                                                 | Name of the catalog, default is `iceberg`                                                                             |
| iceberg.catalog.*                                               | Properties passed through to Iceberg catalog initialization                                                           |
| iceberg.hadoop-conf-dir                                         | If specified, Hadoop config files in this directory will be loaded                                                    |
| iceberg.hadoop.*                                                | Properties passed through to the Hadoop configuration                                                                 |
| iceberg.kafka.*                                                 | Properties passed through to control topic Kafka client initialization                                                |

If `routing.strategy` is not set, the connector keeps the existing behavior for backward compatibility:
it uses `dynamic-field` when `iceberg.tables.dynamic-enabled` is `true`, `regex` when
`iceberg.tables.route-field` is set, and `all-tables` otherwise.

The supported routing strategies are:

* `all-tables` - routes every record to every table listed in `iceberg.tables`
* `regex` - routes records by matching `iceberg.tables.route-field` against each table's `route-regex`
* `dynamic-field` - routes records to the table name stored in `iceberg.tables.route-field`
* `topic-to-table` - routes records by matching the Kafka topic name against `iceberg.tables.topic-to-table-mapping`
  or `iceberg.tables.topic-to-table-mapping-file`

For `all-tables` and `regex`, `iceberg.tables` must be specified. For `regex` and `dynamic-field`,
`iceberg.tables.route-field` must be specified. For `topic-to-table`,
exactly one of `iceberg.tables.topic-to-table-mapping` or `iceberg.tables.topic-to-table-mapping-file`
must be specified. The inline mapping format is `topic_name1:table_name1,topic_name2:table_name2`.
The file mapping must be a JSON file with `version` set to `1` and a non-empty `routes`
object whose keys are Kafka topic names and whose values are Iceberg table identifiers.

Records that do not match the selected routing strategy are skipped. For example, a record whose topic is
not present in the configured topic-to-table mapping is not written to any table.

### Kafka configuration

By default the connector will attempt to use Kafka client config from the worker properties for connecting to
the control topic. If that config cannot be read for some reason, Kafka client settings
can be set explicitly using `iceberg.kafka.*` properties.

#### Message format

Messages should be converted to a struct or map using the appropriate Kafka Connect converter.

#### Debezium nanosecond timestamps

Kafka Connect schemas with `INT64` fields named `io.debezium.time.NanoTimestamp` are mapped to
Iceberg `timestamptz_ns` by default. To create selected fields as Iceberg `timestamp_ns` instead,
set `iceberg.tables.schema-timestamp-ns-fields` to `*` or a comma-separated list of field names
or exact field paths.
Examples:

```
"iceberg.tables.schema-timestamp-ns-fields": "event_time"
"iceberg.tables.schema-timestamp-ns-fields": "after.event_time,before.event_time"
"iceberg.tables.schema-timestamp-ns-fields": "*"
"iceberg.tables.schema-timestamp-ns-fields": "payload.event_time"
```

The `*` value matches all Debezium `NanoTimestamp` fields. Other values match either field names or
exact field paths: `event_time` matches any field named `event_time`, including
`event_time`, `after.event_time`, and `payload.meta.event_time`; `payload.event_time` matches only
the exact path `payload.event_time`. Wildcards inside field names or path segments, such as
`*_time_ns` or `payload.*`, are not supported. Iceberg `timestamp_ns` requires table format version
3; for auto-created tables set:

```
"iceberg.tables.auto-create-props.format-version": "3"
```

### Catalog configuration

The `iceberg.catalog.*` properties are required for connecting to the Iceberg catalog. The core catalog
types are included in the default distribution, including REST, Glue, DynamoDB, Hadoop, Nessie,
JDBC, Hive and BigQuery Metastore. JDBC drivers are not included in the default distribution, so you will need to include
those if needed. When using a Hive catalog, you can use the distribution that includes the Hive metastore client,
otherwise you will need to include that yourself.

To set the catalog type, you can set `iceberg.catalog.type` to `rest`, `hive`, or `hadoop`. For other
catalog types, you need to instead set `iceberg.catalog.catalog-impl` to the name of the catalog class.

#### REST example

```
"iceberg.catalog.type": "rest",
"iceberg.catalog.uri": "https://catalog-service",
"iceberg.catalog.credential": "<credential>",
"iceberg.catalog.warehouse": "<warehouse>",
```

#### Hive example

NOTE: Use the distribution that includes the HMS client (or include the HMS client yourself). Use `S3FileIO` when
using S3 for storage and `GCSFileIO` when using GCS (the default is `HadoopFileIO` with `HiveCatalog`).
```
"iceberg.catalog.type": "hive",
"iceberg.catalog.uri": "thrift://hive:9083",
"iceberg.catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
"iceberg.catalog.warehouse": "s3a://bucket/warehouse",
"iceberg.catalog.client.region": "us-east-1",
"iceberg.catalog.s3.access-key-id": "<AWS access>",
"iceberg.catalog.s3.secret-access-key": "<AWS secret>",
```

#### Glue example

```
"iceberg.catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
"iceberg.catalog.warehouse": "s3a://bucket/warehouse",
"iceberg.catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
```

#### Nessie example

```
"iceberg.catalog.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog",
"iceberg.catalog.uri": "http://localhost:19120/api/v2",
"iceberg.catalog.ref": "main",
"iceberg.catalog.warehouse": "s3a://bucket/warehouse",
"iceberg.catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
```

#### BigQuery Metastore example

```
"iceberg.catalog.catalog-impl": "org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog",
"iceberg.catalog.gcp.bigquery.project-id": "my-project",
"iceberg.catalog.gcp.bigquery.location": "us-east1",
"iceberg.catalog.warehouse": "gs://bucket/warehouse",
"iceberg.catalog.io-impl": "org.apache.iceberg.gcp.gcs.GCSFileIO",
"iceberg.tables.auto-create-props.bq_connection": "projects/my-project/locations/us-east1/connections/my-connection",
```

#### Notes

Depending on your setup, you may need to also set `iceberg.catalog.s3.endpoint`, `iceberg.catalog.s3.staging-dir`,
or `iceberg.catalog.s3.path-style-access`. See the [Iceberg docs](https://iceberg.apache.org/docs/latest/) for
full details on configuring catalogs.

### Azure ADLS configuration example

When using ADLS, Azure requires the passing of AZURE_CLIENT_ID, AZURE_TENANT_ID, and AZURE_CLIENT_SECRET for its Java SDK.
If you're running Kafka Connect in a container, be sure to inject those values as environment variables. See the
[Azure Identity Client library for Java](https://learn.microsoft.com/en-us/java/api/overview/azure/identity-readme?view=azure-java-stable) for more information.

An example of these would be:
```
AZURE_CLIENT_ID=e564f687-7b89-4b48-80b8-111111111111
AZURE_TENANT_ID=95f2f365-f5b7-44b1-88a1-111111111111
AZURE_CLIENT_SECRET="XXX"
```
Where the CLIENT_ID is the Application ID of a registered application under
[App Registrations](https://portal.azure.com/#view/Microsoft_AAD_RegisteredApps/ApplicationsListBlade), the TENANT_ID is
from your [Azure Tenant Properties](https://portal.azure.com/#view/Microsoft_AAD_IAM/TenantProperties.ReactView), and
the CLIENT_SECRET is created within the "Certificates & Secrets" section, under "Manage" after choosing your specific
App Registration. You might have to choose "Client secrets" in the middle panel and the "+" in front of "New client secret"
to generate one. Be sure to set this variable to the Value and not the Id.

It's also important that the App Registration is granted the Role Assignment "Storage Blob Data Contributor" in your
Storage Account's Access Control (IAM), or it won't be able to write new files there.

Then, within the Connector's configuration, you'll want to include the following:

```
"iceberg.catalog.type": "rest",
"iceberg.catalog.uri": "https://catalog:8181",
"iceberg.catalog.warehouse": "abfss://storage-container-name@storageaccount.dfs.core.windows.net/warehouse",
"iceberg.catalog.io-impl": "org.apache.iceberg.azure.adlsv2.ADLSFileIO",
"iceberg.catalog.include-credentials": "true"
```

Where `storage-container-name` is the container name within your Azure Storage Account, `/warehouse` is the location
within that container where your Apache Iceberg files will be written by default (or if iceberg.tables.auto-create-enabled=true),
and the `include-credentials` parameter passes along the Azure Java client credentials along. This will configure the
Iceberg Sink connector to connect to the REST catalog implementation at `iceberg.catalog.uri` to obtain the required
Connection String for the ADLSv2 client

### Google GCS configuration example

By default, Application Default Credentials (ADC) will be used to connect to GCS. Details on how ADC works can
be found in the [Google Cloud documentation](https://cloud.google.com/docs/authentication/application-default-credentials).

```
"iceberg.catalog.type": "rest",
"iceberg.catalog.uri": "https://catalog:8181",
"iceberg.catalog.warehouse": "gs://bucket-name/warehouse",
"iceberg.catalog.io-impl": "org.apache.iceberg.google.gcs.GCSFileIO"
```

### Hadoop configuration

When using HDFS or Hive, the sink will initialize the Hadoop configuration. First, config files
from the classpath are loaded. Next, if `iceberg.hadoop-conf-dir` is specified, config files
are loaded from that location. Finally, any `iceberg.hadoop.*` properties from the sink config are
applied. When merging these, the order of precedence is sink config > config dir > classpath.

### Table statistics

With statistics on, every data commit is followed by a statistics commit for the new snapshot: a Puffin file
with an `apache-datasketches-theta-v1` blob (property `ndv`) per column, merged incrementally with the parent's,
and optionally a partition statistics file. Off by default. A failed statistics step only logs a warning: the
data commit and offsets are not affected, and the statistics are caught up later.

#### Statistics settings

Keys are `iceberg.tables.statistics.<key>` and, per table (dynamic tables too),
`iceberg.table.<table name>.statistics.<key>`. An invalid value fails the connector at start.

| Key                          | Type             | Default                               | Description                                                              |
|------------------------------|------------------|---------------------------------------|--------------------------------------------------------------------------|
| `enabled`                    | boolean          | `false`                               | NDV statistics; also per table                                           |
| `partition-stats-enabled`    | boolean          | `false`                               | Partition statistics of partitioned tables, not ORC; also per table      |
| `lg-nominal-entries`         | int, 4 to 16     | `12`                                  | log2 of the sketch size                                                  |
| `gap-max-files`              | int, 0 or more   | `1000`                                | Max data files read to catch up snapshots without statistics             |
| `gap-max-bytes`              | long, 0 or more  | `10737418240` (10 GiB)                | Max total size of those files                                            |
| `retain-snapshots`           | int, -1 or 1+    | `10`                                  | N latest statistics entries of each kind kept on the commit branch, `-1` keeps all |
| `health-recheck-interval-ms` | long, 0 or more  | `600000` (10 min)                     | Recheck of a quiet table with a raised metric, `0` turns it off          |
| `stale-deleted-ratio`        | double, above 0  | `0.2`                                 | Share of sketched rows no longer live that makes the statistics stale    |
| `columns`                    | comma-separated  | first 100 primitive top-level columns | Columns to collect NDV for; per table only                               |

```json
"iceberg.tables.statistics.enabled": "true",
"iceberg.table.db.events.statistics.partition-stats-enabled": "true",
"iceberg.table.db.audit.statistics.enabled": "false",
"iceberg.table.db.wide.statistics.columns": "id,customer_id,status"
```

* `lg-nominal-entries` above 12 helps only statistics the connector starts on an empty table: engines write 12.
  Lowering it needs a full recompute. A task holds a sketch per column and table: up to 64 KiB at 12, 1 MiB at 16.
* `columns` are looked up by name. A column added to the list of a table with statistics needs `ANALYZE` of
  that column; rename a column and the list together, or it drops out.
* `staging-orphan-ttl-ms` must exceed `iceberg.control.commit.interval-ms` plus `iceberg.control.commit.timeout-ms`.
* Engines use the statistics with Spark `spark.sql.cbo.enabled=true` and Trino
  `iceberg.extended-statistics.enabled=true` (the default).

#### Runbook

| Case                                                                | Action                                                                                                                                  |
|---------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------|
| Statistics enabled on a non-empty table (`missing-baseline`)        | Once: Trino `ANALYZE db.events` (required with `uuid` columns) or Spark `CALL catalog.system.compute_table_stats(table => 'db.events')` |
| Gap over the limit (`gap-over-limit`)                               | Full `ANALYZE` / `compute_table_stats`, or raise `gap-max-files` / `gap-max-bytes`                                                      |
| Column type widened, for example `int` to `long`                    | `ANALYZE db.events WITH (columns = ARRAY['c'])`                                                                                         |
| Partition statistics to be enabled on a large table                 | First Spark `CALL catalog.system.compute_partition_stats(table => 'db.events')`                                                         |
| Warning `NDV of change set X … is lost` (copy-on-write)             | `ANALYZE`                                                                                                                               |
| Time travel or rollback beyond the last N snapshots with statistics | `ANALYZE` (in copy-on-write a drain is one snapshot)                                                                                    |

* Blobs of an external `ANALYZE` for columns outside `columns` disappear after the next connector commit.
  Spark `compute_table_stats` with `columns` drops the blobs of the other columns.
* Trino `OPTIMIZE` and partial `ANALYZE` copy older blobs; if data was added since, the connector ignores the
  copy. Trino `INSERT` merges with the nearest snapshot with statistics, not the parent, and may miss values
  of other writers.
* Spark hashes a `uuid` as a string, the connector and Trino as bytes: on a sketch built by Spark the NDV can
  be up to twice too high until a full `ANALYZE` in Trino.
* `metadata.json` grows by about N x (250 x columns + 500) bytes, ~55 KiB at 20 columns and N = 10, and every
  cycle adds one more metadata version. Use `write.metadata.delete-after-commit.enabled=true`,
  `write.metadata.compression-codec=gzip`, a smaller N and an explicit `columns` list.
* The connector does not clean `metadata/`: files of an entry replaced by a concurrent `ANALYZE` of the head,
  and files left by a crash or a failed write (partition statistics too), are removed by `remove_orphan_files`.

#### Metrics

Registered by the task hosting the coordinator (`kafka.connect:type=plugins,connector=<name>,task=<id>`),
Kafka Connect 4.1 or later. Alert on a value above 0 over a window longer than
`iceberg.control.commit.interval-ms`; the warning in the log names the table.

| Metric                               | Tables                                         | Action                                                    |
|--------------------------------------|------------------------------------------------|-----------------------------------------------------------|
| `statistics-tables-gap-over-limit`   | gap over the limit                             | Full `ANALYZE`, or raise the limit                        |
| `statistics-tables-missing-baseline` | without statistics, or with columns without it | `ANALYZE` of the table or of the columns from the warning |
| `statistics-tables-stale`            | NDV over `stale-deleted-ratio`                 | `ANALYZE`                                                 |

Staleness is hidden by a Trino `INSERT` (it resets the share), and shows only after compaction in merge-on-read
upsert mode and for deletes in format version 2. After upgrading a table to version 3, run Spark
`rewrite_position_delete_files` with `options => map('rewrite-all', 'true')`, then recompute.

#### Limits

* **Coordinator heap:** a catch-up step needs ~130-230 MiB (a Parquet row group per file plus sketches). Up to
  two lag steps run at once; when other engines write to the connector's tables, up to
  `iceberg.control.commit.threads` steps.
* **`iceberg.control.commit.threads`** of 2 or more: with one thread a catch-up blocks other tables.
* **Load:** one more metadata commit per cycle. The INFO line ending a step shows files read; growth means
  foreign writers or no right to write Puffin files.
* **Restart:** only static tables (`iceberg.tables`, `iceberg.tables.topic-to-table-mapping`) keep their
  statistics lag; a dynamic table catches up with its next commit.
* **Staging:** Puffin files are deleted after their step or after `staging-orphan-ttl-ms`. In merge-on-read
  they can outlive the TTL after a restart until the table gets data, and are never cleaned once statistics
  are turned off: delete `<staging>/<namespace>.<table>/<connect group id>/stats/` by hand or with a lifecycle
  rule. The same applies to storage without file creation time (GCS without `createTime`) and to dropped tables.

#### Permissions

Tasks write to `<staging>/<namespace>.<table>/<connect group id>/stats/`; the coordinator reads, lists and
deletes there, and writes and deletes in the table's `metadata/`. `<staging>` is
`iceberg.tables.staging-location`; `.`, `/` and `%` in names are written as `%2E`, `%2F` and `%25`. Without
the right to write Puffin files a task logs a warning per table each cycle and the coordinator reads data files
instead.

#### Upgrade and rollback

* **Upgrade:** upgrade all tasks, then enable statistics.
* **Rollback to a release without copy-on-write:** turn statistics off, wait for
  `Commit … complete, committed to N of N table(s)` without `(partial)`, then roll back.

#### Statistics in copy-on-write

* The statistics step of the final snapshot delays valid-through and its `CommitToTable` by seconds, minutes
  when catching up a gap. Slices get no statistics.
* Staging keeps the change set until its final step succeeds. After switching to merge-on-read before that,
  delete `<staging>/<namespace>.<table>/<connect group id>/_changesets/<X>.json` and the files in its
  `stagedFiles`, where X is `kafka.connect.copy-on-write.final-change-set-id` of the latest copy-on-write snapshot.

### Compatibility with earlier releases

With copy-on-write or statistics on, the control topic gets events that releases without copy-on-write cannot
decode. Their tasks on that topic fail: during a rolling upgrade, after a rollback, and in other connectors
sharing it. Workers fail with `Unknown field ordinal` or `ArrayIndexOutOfBoundsException`, the coordinator with
`NotRunningException: Coordinator unexpectedly terminated` after every restart. Give each connector its own
`iceberg.control.topic`.

Finish the upgrade to recover. If an earlier release has to run, stop whatever writes new events (see
[Upgrade and rollback](#upgrade-and-rollback)), stop the failing connector and skip the events; their records
are lost unless the source topics are read again:

```bash
kafka-consumer-groups --bootstrap-server localhost:9092 --group <connect group id>-coord \
  --topic control-iceberg --reset-offsets --to-latest --execute
```

`<connect group id>` is `iceberg.connect.group-id`, by default `connect-<connector name>`.

## Examples

### Initial setup

#### Source topic

This assumes the source topic already exists and is named `events`.

#### Control topic

If your Kafka cluster has `auto.create.topics.enable` set to `true` (the default), then the control topic will be
automatically created. If not, then you will need to create the topic first. The default topic name is `control-iceberg`:
```bash
bin/kafka-topics.sh  \
  --command-config command-config.props \
  --bootstrap-server ${CONNECT_BOOTSTRAP_SERVERS} \
  --create \
  --topic control-iceberg \
  --partitions 1
```
*NOTE: Clusters running on Confluent Cloud have `auto.create.topics.enable` set to `false` by default.*

#### Iceberg catalog configuration

Configuration properties with the prefix `iceberg.catalog.` will be passed to Iceberg catalog initialization.
See the [Iceberg docs](https://iceberg.apache.org/docs/latest/) for details on how to configure
a particular catalog.

### Single destination table

This example writes all incoming records to a single table.

#### Create the destination table

```sql
CREATE TABLE default.events (
    id STRING,
    type STRING,
    ts TIMESTAMP,
    payload STRING)
PARTITIONED BY (hours(ts))
```

#### Connector config

This example config connects to a Iceberg REST catalog.
```json
{
  "name": "events-sink",
  "config": {
    "connector.class": "org.apache.iceberg.connect.IcebergSinkConnector",
    "tasks.max": "2",
    "topics": "events",
    "iceberg.tables": "default.events",
    "iceberg.catalog.type": "rest",
    "iceberg.catalog.uri": "https://localhost",
    "iceberg.catalog.credential": "<credential>",
    "iceberg.catalog.warehouse": "<warehouse name>"
  }
}
```

### Multi-table fan-out, static routing

This example writes records with `type` set to `list` to the table `default.events_list`, and
writes records with `type` set to `create` to the table `default.events_create`. Other records
will be skipped.

#### Create two destination tables

```sql
CREATE TABLE default.events_list (
    id STRING,
    type STRING,
    ts TIMESTAMP,
    payload STRING)
PARTITIONED BY (hours(ts));

CREATE TABLE default.events_create (
    id STRING,
    type STRING,
    ts TIMESTAMP,
    payload STRING)
PARTITIONED BY (hours(ts));
```

#### Connector config

```json
{
  "name": "events-sink",
  "config": {
    "connector.class": "org.apache.iceberg.connect.IcebergSinkConnector",
    "tasks.max": "2",
    "topics": "events",
    "iceberg.tables": "default.events_list,default.events_create",
    "iceberg.tables.route-field": "type",
    "iceberg.table.default.events_list.route-regex": "list",
    "iceberg.table.default.events_create.route-regex": "create",
    "iceberg.catalog.type": "rest",
    "iceberg.catalog.uri": "https://localhost",
    "iceberg.catalog.credential": "<credential>",
    "iceberg.catalog.warehouse": "<warehouse name>"
  }
}
```

### Multi-table fan-out, dynamic routing

This example writes to tables with names from the value in the `db_table` field. If a table with
the name does not exist, then the record will be skipped. For example, if the record's `db_table`
field is set to `default.events_list`, then the record is written to the `default.events_list` table.

#### Create two destination tables

See above for creating two tables.

#### Connector config

```json
{
  "name": "events-sink",
  "config": {
    "connector.class": "org.apache.iceberg.connect.IcebergSinkConnector",
    "tasks.max": "2",
    "topics": "events",
    "iceberg.tables.dynamic-enabled": "true",
    "iceberg.tables.route-field": "db_table",
    "iceberg.catalog.type": "rest",
    "iceberg.catalog.uri": "https://localhost",
    "iceberg.catalog.credential": "<credential>",
    "iceberg.catalog.warehouse": "<warehouse name>"
  }
}
```

### Topic-to-table routing

This example writes records from the Kafka topic `sales-topic` to the Iceberg table `default.sales`
and records from `logs-topic` to `default.logs`. Records from topics that are not present in
`iceberg.tables.topic-to-table-mapping` are skipped.

#### Create two destination tables

```sql
CREATE TABLE default.sales (
    id STRING,
    ts TIMESTAMP,
    payload STRING)
PARTITIONED BY (hours(ts));

CREATE TABLE default.logs (
    id STRING,
    ts TIMESTAMP,
    payload STRING)
PARTITIONED BY (hours(ts));
```

#### Connector config

```json
{
  "name": "events-sink",
  "config": {
    "connector.class": "org.apache.iceberg.connect.IcebergSinkConnector",
    "tasks.max": "2",
    "topics": "sales-topic,logs-topic",
    "routing.strategy": "topic-to-table",
    "iceberg.tables.topic-to-table-mapping": "sales-topic:default.sales,logs-topic:default.logs",
    "iceberg.catalog.type": "rest",
    "iceberg.catalog.uri": "https://localhost",
    "iceberg.catalog.credential": "<credential>",
    "iceberg.catalog.warehouse": "<warehouse name>"
  }
}
```

For large mappings, use `iceberg.tables.topic-to-table-mapping-file` instead of storing all routes
in the connector config. The file must be present at the same absolute path on every Kafka Connect
worker. It is read when the task starts; changes to the file are not reloaded until the connector
is restarted or reconfigured.

Example mapping file:

```json
{
  "version": 1,
  "routes": {
    "sales-topic": "default.sales",
    "logs-topic": "default.logs"
  }
}
```

Example connector config using the file:

```json
{
  "name": "events-sink",
  "config": {
    "connector.class": "org.apache.iceberg.connect.IcebergSinkConnector",
    "tasks.max": "2",
    "topics": "sales-topic,logs-topic",
    "routing.strategy": "topic-to-table",
    "iceberg.tables.topic-to-table-mapping-file": "/etc/iceberg/topic-table-routes.json",
    "iceberg.catalog.type": "rest",
    "iceberg.catalog.uri": "https://localhost",
    "iceberg.catalog.credential": "<credential>",
    "iceberg.catalog.warehouse": "<warehouse name>"
  }
}
```

### Change data capture

This example applies inserts, updates, and deletes based on the value of a field in the record.
For example, if the `cdc-field` is set to `I` or `R` then the record is inserted, if `U` then it is
upserted, and if `D` then it is deleted. This requires that the table `format-version` to be greater than 2.
The Iceberg identifier field(s) are used to identify a row, if that is not set for the table,
then the `iceberg.tables.default-id-columns` or `iceberg.table.\<table name\>.id-columns` configuration
can be set instead. CDC can be combined with multi-table fan-out.

CDC mode writes equality deletes to handle updates and deletes. During reads, the query engine must
apply equality deletes by scanning data files that may contain matching rows based on the identifier columns.

#### Row-level mode

By default the connector applies updates and deletes in `merge-on-read` mode, as described above: it writes
delete files and the query engine merges them on read. This differs from the Iceberg default for
`write.update.mode`, `write.delete.mode` and `write.merge.mode`, which is `copy-on-write`. These table
properties do not select the connector's mode: in `merge-on-read` a mismatch is only logged as a warning.

Set `iceberg.tables.row-level-mode` to `copy-on-write` to rewrite the affected data files instead. The table
then contains only data files, and engines without delete file support read it correctly. The mode applies
to all tables of the connector; to write some tables in `copy-on-write` and others in `merge-on-read`, use
separate connector instances. In `copy-on-write`:

* `iceberg.tables.cdc-field` or `iceberg.tables.upsert-mode-enabled` is required, and identifier fields
  must be top-level columns
* a table declaring `merge-on-read` in `write.*.mode` fails the task with a `ConfigException`
* the coordinator plans the affected files on each commit and the tasks rewrite them in parallel, so
  `tasks.max` and `iceberg.tables.copy-on-write.rewrite-threads` set the rewrite throughput
* the cost of a commit grows with the size of the affected data files, not with the number of changes;
  a very small `iceberg.control.commit.interval-ms` increases write amplification
* a change set larger than `max-slice-keys` or `max-rewrite-bytes` is applied over several commits;
  every commit is atomic and none of them adds delete files
* a row is found by its identifier fields in any partition, so a delete carrying only the key and an
  update moving a row to another partition are applied correctly, and an insert of an existing key
  replaces the row instead of adding a duplicate; row counts may differ from a `merge-on-read` copy

Switching a table from `merge-on-read` to `copy-on-write` needs no migration: delete files written before
stay valid until compaction. Before switching back, let the connector apply the pending changes: while a
`copy-on-write` change set is unfinished, `merge-on-read` commits to that table are rejected.

Run `remove_orphan_files` regularly: files of a failed or cancelled rewrite may stay behind, as after an
aborted Spark write. Keep its `older_than` window longer than applying a change set takes: staged change
files are not referenced by the table metadata until the change set is fully applied.

#### Production recommendations

**Compaction is required**: For production CDC workloads, periodic compaction is essential to maintain
query performance. Compaction merges equality deletes with their corresponding data files, reducing
the number of delete files that need to be processed during reads.

**Table partitioning**: Proper partitioning significantly reduces the scan overhead of equality deletes.
When a table is partitioned, equality deletes only need to be applied to data files within the same
partition. Choose partition columns that align with your CDC data patterns (e.g., date columns for
time-series data).
**Identifier column selection**: The identifier columns define which rows are matched for updates and
deletes. These should be:

* Unique or form a composite unique key for the data
* Included in the table's partition spec when possible to limit delete scope

#### Compaction

Run compaction periodically to merge equality deletes with data files. This can be done using Spark:

```sql
-- Run compaction on the table
CALL catalog_name.system.rewrite_data_files('db.events')

-- Compaction with specific options
CALL catalog_name.system.rewrite_data_files(
  table => 'db.events',
  options => map('delete-file-threshold', '10')
)

-- Remove orphan delete files after compaction
CALL catalog_name.system.rewrite_deletes('db.events')
```

Or using the Iceberg Actions API:

```java
SparkActions.get(spark)
    .rewriteDataFiles(table)
    .option("delete-file-threshold", "10")
    .execute();
```

For automated compaction, consider scheduling these operations via a workflow orchestrator or
using managed Iceberg services that provide automatic compaction.

#### Create the destination table
See above for creating the table

#### Connector config
```json
{
"name": "events-sink",
"config": {
    "connector.class": "org.apache.iceberg.connect.IcebergSinkConnector",
    "tasks.max": "2",
    "topics": "events",
    "iceberg.tables": "default.events",
    "iceberg.tables.cdc-field": "_cdc_op",
    "iceberg.tables.cdc.op.insert": "c",
    "iceberg.tables.cdc.op.update": "u",
    "iceberg.tables.cdc.op.delete": "r",
    "iceberg.catalog.type": "rest",
    "iceberg.catalog.uri": "https://localhost",
    "iceberg.catalog.credential": "<credential>",
    "iceberg.catalog.warehouse": "<warehouse name>"
    }
}
```

## SMTs for the Apache Iceberg Sink Connector

This project contains some SMTs that could be useful when transforming Kafka data for use by
the Iceberg sink connector.

### CopyValue
_(Experimental)_

The `CopyValue` SMT copies a value from one field to a new field.

#### Configuration

| Property         | Description       |
|------------------|-------------------|
| source.field     | Source field name |
| target.field     | Target field name |

#### Example

```
"transforms": "copyId",
"transforms.copyId.type": "org.apache.iceberg.connect.transforms.CopyValue",
"transforms.copyId.source.field": "id",
"transforms.copyId.target.field": "id_copy",
```

### DmsTransform
_(Experimental)_

The `DmsTransform` SMT transforms an AWS DMS formatted message for use by the sink's CDC feature.
It will promote the `data` element fields to top level and add the following metadata fields:
`_cdc.op`, `_cdc.ts`, and `_cdc.source`.

##### Configuration

The SMT currently has no configuration.

### DebeziumTransform
_(Experimental)_

The `DebeziumTransform` SMT transforms a Debezium formatted message for use by the sink's CDC feature.
It will promote the `before` or `after` element fields to top level and add the following metadata fields:
`_cdc.op`, `_cdc.ts`, `_cdc.offset`, `_cdc.source`, `_cdc.target`, and `_cdc.key`.

##### Configuration

| Property            | Description                                                                       |
|---------------------|-----------------------------------------------------------------------------------|
| cdc.target.pattern  | Pattern to use for setting the CDC target field value, default is `{db}.{table}`  |

### JsonToMapTransform
_(Experimental)_

The `JsonToMapTransform` SMT parses Strings as Json object payloads to infer schemas.  The iceberg-kafka-connect
connector for schema-less data (e.g. the Map produced by the Kafka supplied JsonConverter) is to convert Maps into Iceberg
Structs.  This is fine when the JSON is well-structured, but when you have JSON objects with dynamically
changing keys, it will lead to an explosion of columns in the Iceberg table due to schema evolutions.

This SMT is useful in situations where the JSON is not well-structured, in order to get data into Iceberg where
it can be further processed by query engines into a more manageable form. It will convert nested objects to
Maps and include Map type in the Schema.  The connector will respect the Schema and create Iceberg tables with Iceberg
Map (String) columns for the JSON objects.

Note:

- You must use the `stringConverter` as the `value.converter` setting for your connector, not `jsonConverter`
    - It expects JSON objects (`{...}`) in those strings.
- Message keys, tombstones, and headers are not transformed and are passed along as-is by the SMT

##### Configuration

| Property             | Description  (default value)             |
|----------------------|------------------------------------------|
| json.root | (false) Boolean value to start at root   |

The `transforms.IDENTIFIER_HERE.json.root` is meant for the most inconsistent data.  It will construct a Struct with a single field
called `payload` with a Schema of `Map<String, String>`.

If `transforms.IDENTIFIER_HERE.json.root` is false (the default), it will construct a Struct with inferred schemas for primitive and
array fields.  Nested objects become fields of type `Map<String, String>`.

Keys with empty arrays and empty objects are filtered out from the final schema.  Arrays will be typed unless the
json arrays have mixed types in which case they are converted to arrays of strings.

Example json:

```json
{
  "key": 1,
  "array": [1,"two",3],
  "empty_obj": {},
  "nested_obj": {"some_key": ["one", "two"]}
}
```

Will become the following if `json.root` is true:

```
SinkRecord.schema: 
  "payload" : (Optional) Map<String, String>
  
Sinkrecord.value (Struct): 
  "payload"  : Map(
    "key" : "1",
    "array" : "[1,"two",3]"
    "empty_obj": "{}"
    "nested_obj": "{"some_key":["one","two"]}"
  )
```

Will become the following if `json.root` is false

```
SinkRecord.schema: 
  "key": (Optional) Int32,
  "array": (Optional) Array<String>,
  "nested_object": (Optional) Map<string, String>
  
SinkRecord.value (Struct):
  "key" 1, 
  "array" ["1", "two", "3"] 
  "nested_object" Map ("some_key" : "["one", "two"]") 
```

### KafkaMetadataTransform
_(Experimental)_

The `KafkaMetadata` injects `topic`, `partition`, `offset`, `timestamp` which are properties are the Kafka message.

#### Configuration

| Property       | Description (default value)                                                       |
|----------------|-----------------------------------------------------------------------------------|
| field_name     | (_kafka_metadata) prefix for fields                                               |
| nested         | (false) if true, nests data on a struct else adds to top level as prefixed fields |
| external_field | (none) appends a constant `key,value` to the metadata (e.g. cluster name)         |

If `nested` is on:

`_kafka_metadata.topic`, `_kafka_metadata.partition`, `_kafka_metadata.offset`, `_kafka_metadata.timestamp`

If `nested` is off:
`_kafka_metadata_topic`, `_kafka_metadata_partition`, `_kafka_metadata_offset`, `_kafka_metadata_timestamp`

### MongoDebeziumTransform
_(Experimental)_

The `MongoDebeziumTransform` SMT transforms a Mongo Debezium formatted message with `before`/`after` BSON
strings into `before`/`after` typed Structs that the `DebeziumTransform` SMT expects.

It does not (yet) support renaming columns if mongodb column is not supported by your underlying
catalog type.

#### Configuration

| Property            | Description                                      |
|---------------------|--------------------------------------------------|
| array_handling_mode  | `array` or `document` to set array handling mode |

Value array (the default) will encode arrays as the array datatype. It is user’s responsibility to ensure that
all elements for a given array instance are of the same type. This option is a restricting one but offers
easy processing of arrays by downstream clients.

Value document will convert the array into a struct of structs in the similar way as done by BSON serialization.
The main struct contains fields named _0, _1, _2 etc. where the name represents the index of the element in the array.
Every element is then passed as the value for the given field.
