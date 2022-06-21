## Apache Beam connector (SDK) for Cognite Data Fusion

Apache Beam is a programming model for streaming and batch data jobs/data pipelines. The jobs can be executed on multiple 
runners like Apache Flink, Apache Spark, Google Dataflow and more. Please refer to 
[the Apache Beam website for more information](https://beam.apache.org/)

The Beam connector provides convenient access to Cognite Data Fusion's capabilities. It covers a large part of CDF's
capability surface, including experimental features. The connector tries to follow the Apache Beam idioms for a good 
developer experience.

Some of the connector's capabilities:
- _Upsert support_. It will automatically handle `create`and `update` for you.
- _Retries with backoff_. Transient failures will automatically be retried.
- _Performance optimization_. The connector will handle batching and parallelization of requests per worker.

The Apache Beam connector is built on top of the [Java SDK for Cognite Data Fusion](https://github.com/cognitedata/cdf-sdk-java) 
and follows the structure of the SDK, which again follows the structure of the [CDF REST API](https://docs.cognite.com/api/v1/). 


- [Client configuration. How to setup authentication.](authentication.md)
- [Reading and writing data](readAndWriteData.md)
- [Extraction pipelines](extractionPipelines.md)