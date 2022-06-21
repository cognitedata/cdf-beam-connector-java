## Extraction pipelines

`Extraction pipelines` can be set up in Cognite Data Fusion (CDF) to help monitor data flows publishing data into CDF data sets. Data applications / modules (i.e. extractors, data transformations, contextualization jobs, etc.) that take part in the data flow can register themselves as an `extraction pipeline` and submit monitoring data to CDF.

CDF can offer a status overview, monitor the extraction pipelines and send notifications on data flow interruptions.

The Cognite online documentation offers a good overview to extraction pipelines here: [https://docs.cognite.com/cdf/integration/guides/interfaces/about_integrations](https://docs.cognite.com/cdf/integration/guides/interfaces/about_integrations).

The Beam connector has built-in support for extraction pipeline. There are two, main patterns offered:
- _Automagic_ reporting of successful pipeline runs from the IO writers.
- _Custom_ reporting of pipeline runs.

### Configuring IO writers to submit pipeline runs

The IO writers can automatically submit pipeline runs upon successfully completing their write windows. I.e. they can report a successful run to CDF--but they are not able to report a failed run. 

For batch pipelines, there is usually only a single, global window, so the writer will report a successful run when all writes have been completed. While, for streaming pipelines, there are multiple windows reporting the overall progress/heartbeat of the pipeline.

You enable automatic pipeline run reporting by setting the parameter `withExtractionPipeline(String extractionPipelineExtId)` on the `WriterConfig` and supplying it to the writer:

```java
// Just illustrating the sink part of the pipeline
PCollection<Event> eventsToWrite = pipeline.apply(<produced-by-your-upstream-pipeline>);

// Configure the CDF event writer to report extraction pipeline runs.
// The pipeline runs will be reported as "success" when the writer has successfully completed its writes.        
eventsToWrite        
        .apply("write events", CogniteIO.writeEvents()                  // The CDF event IO writer
                .withProjectConfig(<projectConfigClientCredentials>)    // The CDF authentication credentials for the writer  
                .withWriterConfig(WriterConfig.create()                 // The WriterConfig object controlling writer behavior
                        .withExtractionPipeline("extractionPipelineExtId"))   // Enable automatic status reporting to extraction pipeline.
        );

// If you want the writer to report the pipeline runs as "seen" instead of "success", you can add
// an extra configuration parameter to the WriterConfig object.
eventsToWrite
        .apply("write events", CogniteIO.writeEvents()                  // The CDF event IO writer
                .withProjectConfig(<projectConfigClientCredentials>)    // The CDF authentication credentials for the writer  
                .withWriterConfig(WriterConfig.create()                 // The WriterConfig object controlling writer behavior
                        .withExtractionPipeline("extractionPipelineExtId", ExtractionPipelineRun.Status.SEEN))
        );
// ExtractionPipelineRun.Status.SEEN specifies the status the writer will use when reporting runs
```

### Custom reporting of pipeline runs

You can also configure custom reporting of extraction pipeline runs via a dedicated `transform`: `com.cognite.beam.io.transform.extractionPipelines.CreateRun`. CreateRun takes a collection of `ExtractionPipelineRun` objects as input and writes them to Cognite Data Fusion. 

When using this transform, you can tailor the pipeline run reporting to your liking--including when and under which conditions the run(s) are reported. 

```java
// Just illustrating how ExtractionPipelineRunObjects are built. You probably want to construct them more dynamically
// in your pipeline.
PCollection<ExtractionPipelineRun> pipelineRunCollection = pipeline
        .apply("Build run objects", Create.of(
                ExtractionPipelineRun.newBuilder()
                        .setExternalId("The extraction pipeline external id")
                        .setStatus(ExtractionPipelineRun.Status.SUCCESS)
                        .setMessage("My informative status message")
                        .build()
        ));

// Write the extraction pipeline run objects to Cognite Data Fusion        
pipelineRunCollection
        .apply("Write pipeline run", CreateRun.create()
                .withProjectConfig(<projectConfigClientCredentials>)); // The CDF authentication credentials for writing to CDF
```