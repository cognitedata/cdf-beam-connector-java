# Release log

All notable changes to this project will be documented in this file. 
Changes are grouped as follows:
- `Added` for new features.
- `Changed` for changes in existing functionality.
- `Deprecated` for soon to be removed functionality.
- `Removed` for removed functionality.
- `Fixed` for any bugfixes.
- `Security` in case of vulnerabilities.

## [Planned]

### Long term

- CDF as a SQL source. Beam SQL cli support.

### Medium term

- Publish job metrics to CDF time series.

### Short term

- OOTB incremental read support for time series.

## [0.9.40] 2022-10-03

### Added

- Factory methods `ProjectConfig.ofClientCredentials()` to align with style conventions in the Java SDK.

### Changed

- Add checks for blank/empty strings in `ProjectConfig`. A blank string will be treated as if it is not present / null.

## [0.9.39] 2022-09-02

### Added

- Java SDK 1.17.0
- Beam SDK 2.41.0
- Configurable timeout of the async API endpoints (contextualization endpoints like `entity matching` and `engineering diagrams`). You configure the timeout via `Hints.withAsyncApiJobTimeout(Duration timeout)`. The default timeout is 20 minutes.

## [0.9.38] 2022-07-06

### Added

- Beam SDK 2.40.0

### Fixed

- Writers' automatic reporting of `extraction pipeline run` will also generate a `run` when zero elements are written to CDF.

## [0.9.37] 2022-06-20

### Added

- Java SDK v1.16.0
  - Geo-location attribute on the `files` resource type is supported.
  - `Sequences` upsert support including modified column schema. The `upsert` functionality includes both modified `sequences headers`/`SequenceMetadata` and `sequences rows`/`SequenceBody`. For more information, please refer to the documentation: [https://github.com/cognitedata/cdf-sdk-java/blob/main/docs/sequence.md#update-sequences](https://github.com/cognitedata/cdf-sdk-java/blob/main/docs/sequence.md#update-sequences) and [https://github.com/cognitedata/cdf-sdk-java/blob/main/docs/sequence.md#insert-rows](https://github.com/cognitedata/cdf-sdk-java/blob/main/docs/sequence.md#insert-rows)
- Beam v2.39.0
- Support for `extraction pipelines`

## [0.9.36] 2022-03-13

### Added

- `Read direct` mode for `FilesMetadata` and `Files`. This read mode will output batches of objects (`List<FileMetadata/FileContainer>`) instead of single
    rows. This can be useful in very high data volume scenarios.
- `Write direct` mode for `FilesMetadata` and `Files`. This allows for bypassing the shuffle and batch stage of the writer
  when you have pre-batched objects. This can offer improved performance when writing very large volumes of data.
- Support custom auth scopes when using Open ID Connect authentication.
- Java SDK v1.13.0
  - fixed file binary download expired URL
  - Added support for S3 as temp storage for file binary
- Beam 2.37.0

### Changed
- API metrics are disabled by default. They can be enabled via `ReaderConfig.enableMetrics(true)` / `WriterConfig.enableMetrics(true)`.

## [0.9.35] 2022-03-01

### Added

- Added convenience methods to the `RequestParameters` object for easier handling of items (by `externalId` or `id`). You can use
  `RequestParameters.withItemExternalIds(String... externalId)` and `RequestParameters.withItemInternalIds(Long... externalId)` to add multiple
  items to the request.
- Java SDK v1.11.0
  - Added `3D Models Revisions`
  - Added `3D File Download`
  - Added `3D Asset Mapping`
  - `EngineeringDiagrams` promoted from experimental to stable. It has the same signature and behavior as before and is
    located under the `contextualization` family: `CogniteClient.contextualization().engineeringDiagrams()`.
  - Add utility class `com.cognite.client.util.RawRows` for working with `RawRow` object. Please refer to
    [the documentation](https://github.com/cognitedata/cdf-sdk-java/blob/main/docs/raw.md) for more information.
- Beam SDK 2.36.0

### Deprecated

- The single item methods `RequestParameters.withItemExternalId(String externalId)` and `RequestParameters.withItemInternalId(Long externalId)`
  have been deprecated in favour of the new multi-item versions.

## [0.9.34] 2022-01-18

### Added

- Improve logging of failed API requests.

## [0.9.33] 2022-01-04

### Added

- Java SDK v1.9.0
  - Added `3D Models`
  - Increased read and write timeouts to match sever-side values.
  - Upsert of `sequenceMetadata` not identifying duplicate entries correctly.

### Fixed

- Write direct can accept all collections implementing `Iterable`.

## [0.9.32] 2021-12-30

### Added

- Beam SDK 2.35.0
- Java SDK v1.8.0:
  - File binary upload use PUT instead of POST
  - Further improvements in the file binary upload robustness.
  - Fix. `SequenceMetadata` upsert now respects the max cell count batch limit.

## [0.9.31] 2021-11-17

### Fixed

- `Read direct` could emit empty batches in certain circumstances. 
- Timeouts when using Google Cloud Storage as temp storage for file binaries.

## [0.9.30] 2021-11-14

### Added

- `Read direct` mode for `RawRow` and `Event`. This read mode will output batches of rows (`List<RawRow/Event>`) instead of single 
rows. This can be useful in very high data volume scenarios. 
- `Write direct` mode for `RawRow` and `Event`. This allows for bypassing the shuffle and batch stage of the writer 
when you have pre-batched objects. This can offer improved performance when writing very large volumes of data.
- Support for `dataSetId` in the `Labels` resource type (Java SDK v1.5.0).

### Fixed

- File binary upload robustness (Java SDK v1.5.0).
- `Labels` using api v1 endpoint (Java SDK v1.5.0).

## [0.9.29] 2021-11-09

### Changed

- Refactored the asset synchronization to use the Java SDK implementation. 

## [0.9.28] 2021-11-08

### Added

- Java SDK v 1.4.0. Brings lots of useful features, like:
  - Improved performance reading `relationships` and `sequences`.
  - Improved stability when working with large-volume file binaries.
  - Added support for including source and target objects when reading `sequences`.
- Added support for grayscale image in engineering diagram SVGs.

### Changed

- `CreateInteractiveDiagram.withTargetView()` has been renamed to `CreateInteractiveDiagram.withEntitiesView()` to 
align with API naming.

### Fixed

- `Files.readAll()` would fail if a file object only consists of a header (and not a binary). 

## [0.9.27] 2021-10-22

### Added

- Support for read `first N` for the readers working towards the `list` endpoints in the API (`assets`, `events`,
 `TS headers`, `sequences headers`, `file headers`, `raw rows` and more). This allows you to quickly sample a 
 limited sub-set from a large results set.

## [0.9.26] 2021-10-12

### Added

- Support for Beam v2.33.0
- Bumped the Java SDK to v1.3.0

## [0.9.25]

### Added

- Support for Beam 2.32.0

### Changed
- The experimental feature `interactive P&IDs` has been refactored into `interactive engineering diagrams` in order to 
be aligned with the new CDF API endpoint. The new transform is available at `CogniteIO.experimental().createInteractiveDiagram()`

## [0.9.24]
We skip a few versions due to updating our build pipeline.

### Added

- Support for Beam 2.31.0
- Configurable batch size for file binary download and upload

### Changed

- Breaking change: Remove the use of wrapper objects from the data transfer objects (`Asset`, `Event`, etc.). Please
  refer to the [documentation](https://github.com/cognitedata/cdf-sdk-java/blob/main/docs/readAndWriteData.md#migrating-from-sdk-099)
  for more information.

## [0.9.19]

### Fixed

- Repeated annotations when generating interactive P&IDs.


## [0.9.18]

### Fixed

- TLS / SSL handshake errors when running on Dataflow and connecting to Google APIs. This was due to Dataflow disabling 
  a set of crypto algorithms on its workers (because of poor Java 8 implementations). Now that we are on Java 11, we 
  override the Dataflow config and allow all Java 11 algorithms.

## [0.9.17]

### Added

- Support for Java 11.
- Support for Beam 2.29.0.
- CDF Java SDK 0.9.4.
- Support for authenticating towards Cognite Data Fusion using OpenID Connect.

### Fixed

- Null pointer when writing large file binaries. Under certain circumstances writing large file binaries to CDF 
could result in an error (null pointer). This could happen if the network connection to CDF was interrupted while transferring
  the file binary.

## [0.9.16]

### Fixed

- Duplicates when reading `file header`.

## [0.9.15]

### Added

- Refactored the core I/O engine into a separate Java SDK. This should also give a general performance improvement of about 2x.

- Support for Beam 2.28.0

### Changed

- Refactored `RequestParameters` from `com.cognite.beam.servicesV1.RequestParameters` to `com.cognite.beam.RequestParameters`.
  All other signatures are the same as before, so you may run a search & replace to update your client.

- Refactored data transfer objects from `com.cognite.beam.io.dto` to `com.cognite.client.dto`. 
  All other signatures are the same as before, so you may run a search & replace to update your client.

- Refactored `com.cognite.beam.io.servicesV1` to `com.cognite.client.servicesV1`. 
  All other signatures are the same as before, so you may run a search & replace to update your client.
  
- Refactored `com.cognite.beam.io.config.UpsertMode` to `com.cognite.client.config.UpsertMode`.
  All other signatures are the same as before, so you may run a search & replace to update your client.
  
- `EntityMatch.matchTo` renamed to `EntityMatch.target` to align with the entity matching api v1.

- Experimental: The interactive P&ID service has been updated to use the new entities specification.
  
### Fixed

- Fixed a bug causing the number of write shards to be double of the configured value.

- Fixed missing duplicate detection when upserting sequences rows. 

- Fixed a bug when reading `Relationship` where `TargetExternalId` would always be set to `null`.

- Fixed a null pointer exception when using GCS temp storage for files--in some cases GCS is unable to report a correct content size.

## [0.9.14]

### Fixed

- Update replace sequences with empty metadata.

## 0.9.13

### Added

- Support for `read by id` for `assets`, `events`, `time series`, `sequences` and `files`.
- Support for Beam 2.26.0

### Changed

- Renamed `CogniteIO.readFilesBinariesById` to `CogniteIO.readAllFilesBinariesById` in order to be consistent with the `read` vs. `readAll` pattern.

### Fixed

- Null values in sequences. Sequences now support null values.

## 0.9.12

### Added

- Support for reading and writing `sequences`.  
- Support read `aggregates` for `assets`, `events`, `time series`, `files` and `sequences`.
- Support Beam 2.25.0

## 0.9.11

### Added

- Support for (very) large file binaries via temp blob storage. 
  - Google Cloud Storage and local file storage are supported as temp storage.

### Changed

- New endpoints for entity matcher train and predict.
- New naming for configuration of entity mather and P&ID transforms.

## 0.9.10

### Added

- Relationships in beta.

### Changed

- Entity matcher runs towards the new entity matching endpoints in playground.

## 0.9.9

### Added 

- Improved logging when building interactive P&IDs and when writing files.

## 0.9.8

### Added

- Labels support for files.
- `File.directory` as new attribute on `File`

### Changed

- Create interactive P&ID runs towards the new P&ID api endpoints.
- The create interactive P&ID transform has been refactored with new input type and config options.

### Fixed

- Entity matcher api error messages propagation.
- File writer handles files with >1k asset links.
- File writer handles files with empty file binary.

## 0.9.7

### Added

- New `readDirect` and `writeDirect` modes for time series points. These modes bypass some of the built-in data
validation and optimization steps--this allows for higher performance under some circumstances (for example
if the input data is pre-sorted).

### Changed

- The entity matcher now runs towards the new `fit` and `predict` endpoints.

### Fixed

- Time series writer metrics. 

## 0.9.6

### Added

- Support for entity matching.

### Fixed

- Read and write metrics.

## 0.9.4

### Added

- Generate interactive P&IDs (experimental)
- Experimental support for streaming reads from Raw, Assets, Events and Files.
- Support for security categories on files.
- Support for Labels


## 0.9.3

### Fixed

- Reading from Raw in combination with GCP Secret Manager.

## 0.9.2

### Fixed

- Not fetching secret from GCP Secret Manager.

## 0.9.1

### Added

- Add support for `Relationships`.

### Changed

- `GcpSecretManager` instantiation via `of(projectId, secretId)` instead of `create()`. To highlight the needed input properties.

## 0.9.0

### Added

- Experimental: Streaming support for reading time series datapoints.
- Experimental: Optimized code path for reading time series datapoints.
- Support for GCP secret manager.
- Add datasets as a resource type.
- `Asset` and `AssetLookup` has added support for aggregated properties (`childCount`, `depth` and `path`).
- Add support for the aggregates endpoint for `Asset`, `Event`, `Timeseries`and `File`.

### Changed

- `legacyName` is no longer included when creating time series.
- The `BuildAssetLookup` transform will include aggregated properties in the output `AssetLookup` objects. 
- Support for Beam 2.20.0 

### Fixed

- Data set id included as a comparison attribute for the synchronize assets function.


## 0.8.5

### Fixed

- Upsert items is more robust against race conditions (on large scale upserts) and legacy name collisions.

## 0.8.4

### Added

- Support for data sets for assets, events and time series.

## 0.8.3

### Fixed

- Concurrency error when writing files with more than 2k assetIds.

## 0.8.2

### Added

- Support upsert of files with more than 1k assetIds.

## 0.8.1

### Added

- Utility transform for building read raw table requests based on a config file.

## 0.8.0

### Added

- New request execution framework towards the Cognite API. This enables "fan-out" per worker (multiple parallell async IOs).
- Support for file binaries.
- Support for multi TS read requests. This should speed up wide and shallow queries by 5 - 10x.
- Support for data set for files.

### Fixed

- Default limit for TS points aggregates requests are now set to 10k.

## 0.7.10

### Fixed
- Updated the timeseries point reader to the new proto payload from the Cognite API.
- Read rows from Raw now works in combination with a column specification.

## 0.7.9

### Added

- Add delta read support to TS headers.

### Fixed

- RAW. `minLastUpdatedTime` and `maxLastUpdatedTime`did not work properly in combination with range cursors.

## 0.7.8

### Added

- Files. Support updating `mimeType`.
- CSV reader: Strip out empty lines (lines with no alphabetic characters).

### Fixed

- TS points. Can now resolve duplicates based on `legacyName`.

## 0.7.7

### Added
- CSV reader: Added support for BOM (byte order mark), quoting and custom header.

## 0.7.6

### Added

- Support for reading csv / delimited text files.

## 0.7.5

### Added

- Support for the new TS header advanced filter and partition endpoint.
- Optimized writes for low frequency TS datapoints.

### Fixed

- Api metrics. Fixed batch size mertric when writing timeseries data points.

### Changed

- Changed upper limit on write batch latency.

## 0.7.4

### Added

- Metric for read time series datapoints batch size.
- Utility transforms for parsing TOML config files to map and array.

### Fixed
- `isStep` is added to `TimeseriesPoint`.

## v0.7.3

### Fixed

- Update of time series headers includes `name`.

## v0.7.2

### Fixed

- Delta configuration via parameters and templates.
- `ReadRawRow` configured with `dbName` and/or `tableName` via parameters and templates.

## v0.7.1

### Added

- Metrics can be enabled/disabled via `readerConfig` and `writerConfig`. The default is enabled.

## v0.7.0

### Added

- Delta read support for raw, assets, events and file metadata/header.
- Metrics. Latency and batch size metrics for Cognite API calls.
- API v1 range partitions. Support for API v1 range partitions for assets and events.

### Changed

- Move to Beam 2.16.0

## v0.6.8

### Fixed

- Read single row from raw by row key.

## v0.6.7

### Fixed

- Asset synchronizer: Performance optimization, better delta detection between CDF and source.
- File metadata/header writer. Fixed issue where creating new file metadata could fail.

## v0.6.6

### Fixed

- Asset synchronizer: Fix data integrity check of empty parentExternalId.

## v0.6.5

### Fixed

- Asset synchronizer: Fixed issue with parsing parentExternalId.
- Asset synchronizer: Fixed issue with data validation not handling multiple hierarchies.

## v0.6.4

### Fixed

- Fixed issue with asset input validation where a root node was not identified when the parentExternalId was null (as opposed to an empty string).

## v0.6.3

### Added

- Additional input validation for asset hierarchy synchronization.

## v0.6.2

### Added

- Max write batch size hint for Raw.
- Utility methods for parsing Raw column values into types.
- Upsert and delete file headers/metadata.

## v0.6.1

### Added

- Read TOML files.

## v0.6.0

### Added

- Write and update assets.
- Asset hierarchy synchonization. Continously mirror source, including insert, updates and deletes (for both node metadata and hierarchy structure).
- Support both "partial update" and "replace" as upsert modes.

### Changed

- From ``xxx.builder().build()`` to ``xx.create()`` for user facing config objects and transforms (like ``RequestParameters``, ``ProjectConfig``, etc.).

## v0.5.3

## Fixed

- Fixed issue when writing TS data points and generating a default TS header. In the case of multiple parallel writes there could be collisions. 

## v0.5.2

## Fixed

- Fixed issue when writing TS headers where duplicate ``legacyName`` exists.

## v0.5.1

## Fixed

- Fixed file list / reader error.

## v0.5.0

### Added

- Support auto-create time series headers/metadata for time series where `isString=true`.
- Support for reading file metadata.
- Support for deleting raw rows.

## Changed

- `ReadRows`, the member `lastUpdatedTime` made optional.

### Fixed

- Fixed (another) null pointer exception when using project configuration parameters on the Dataflow runner.

## v0.4.1

### Fixed

- Fixed null pointer exception when using project configuration files.

## v0.4.0

### Added

- Conveniece methods added to ``RequestParameters`` for setting single item ``externalId`` / ``id``.
- Using protobuf for writing and reading time series data points.
- Support for providing ``ProjectConfig`` via file.

### Changed

- New group and artifact ids.
- Beam version: from 2.13.0 to 2.14.0
- ``isString`` is removed from ``TimeseriesPoint``. This field is redundant as the same information can be obtained from ``getValueOneofCase()``.
- ``oneOf`` cases in ``TimeseriesPoint``, ``TimeseriesPointPost`` and ``Item`` has been renamed for increased clarity.

## v0.3.1

### Fixed

- Fix protobuf version conflict.

## v0.3.0

### Added

- Add support for writing and updating time series data points.

## v0.2.9

### Added

- Add support for writing time series meta data.
- Add support for delete time series.
- Add support for delete assets.

### Fixed

- Fix write insert error for objects with existing "id".

## v0.2.8

### Changed

- Increase connection and read timeouts towards the Cognite API to 90 seconds. This is to accomodate reading potentially large response payloads.
- Optimize handling of cursors.

### Fixed

- Fix TS header reader (request provider error).
- Fix support for templates. ``ValueProvider<T>`` is now lazily evaluated.

## v0.2.7

### Added

- Add rootId to assets.
- Add writeShards and maxWriteBatchLatency to Hints.
- Add support for raw cursors on read.
- Add support for listing raw databases and tables.
- Add shortcut methods for raw.dbName and raw.tableName to ``RequestParameters``.
- Add support for specifying raw.dbName and raw.tableName via ``ValueProvider<String>``.

### Changed

- Renamed "splits" to "shards" (Hints object)

### Removed

- Remove depth and path from assets.

## v0.2.6

### Added

- Add batch identifier to writer logs.

### Changed

- Improve the Raw writer and reader.

## v0.2.5

- Refactored the connector configuration.
- Supports API v1.
- Added support for new readers and writers:
  - Readers: Assets, events, TS header and TS datapoints, Raw.
  - Writers: Events and Raw.
- Writers perform automatic upsert.
- Readers support push-down of all filters offered by the API.

## v0.2.1 and earlier

### Added

- Added support for reading time series headers and datapoints.
- New configuration pattern.
- Added support for reading events.
- Configuration of the reader is captured in a configuration object.
