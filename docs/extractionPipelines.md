## Extraction pipelines

`Extraction pipelines` can be set up in Cognite Data Fusion (CDF) to help monitor data flows publishing data into CDF data sets. Data applications / modules (i.e. extractors, data transformations, contextualization jobs, etc.) that take part in the data flow can register themselves as an `extraction pipeline` and submit monitoring data to CDF.

CDF can offer a status overview, monitor the extraction pipelines and send notifications on data flow interruptions.

The Cognite online documentation offers a good overview to extraction pipelines here: [https://docs.cognite.com/cdf/integration/guides/interfaces/about_integrations](https://docs.cognite.com/cdf/integration/guides/interfaces/about_integrations).

The Beam connector has built-in support for extraction pipeline. There are two, main patterns offered:
- _Automagic_ reporting of successful pipeline runs from the IO writers.
- _Custom_ reporting of pipeline runs.

### Configuring IO writers to submit pipeline runs

The IO writers can automatically submit pipeline runs

The Beam Connector follows the Cognite Data Fusion (CDF) REST API structure, so it is helpful to familiarize yourself with 
the Cognite API documentation: [https://docs.cognite.com/api/v1/](https://docs.cognite.com/api/v1/). The connector is 
structured into different operations (`read`, `readAll`, `write`, etc.) working on different resource types/API endpoints 
(`assets`, `events`, `contextualization`, etc.). 

Most operations will consume and/or return some data objects. The Connector uses typed objects based on Protobuf 
([https://developers.google.com/protocol-buffers](https://developers.google.com/protocol-buffers)). The structure 
of the data transfer objects follow the structure of the Cognite API objects.


```java
// Old style, with the wrapper objects
Event oldEvent = Event.newBuilder()
    .setExternalId(StringValue.of("myExternalId"))                  // The String must be wrapped by StringValue
    .build();

if (oldEvent.hasExternalId()) {
    LOG.info("External id: {}", oldEvent.getExternalId.getValue());  // An additional "getValue()" must be used to unpack the String
}

// New pattern, using protobuf's native optional feature
Event newEvent = Event.newBuilder()
    .setExternalId("myExternalId")                          // No wrapper object--just set the String directly
    .build();

if (oldEvent.hasExternalId()) {
    LOG.info("External id: {}", newEvent.getExternalId()) ; // No "getValue()"--just get the String directly
}
```
