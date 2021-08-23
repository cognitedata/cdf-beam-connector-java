## Reading and writing data from/to Cognite Data Fusion

The Beam Connector follows the Cognite Data Fusion (CDF) REST API structure, so it is helpful to familiarize yourself with 
the Cognite API documentation: [https://docs.cognite.com/api/v1/](https://docs.cognite.com/api/v1/). The connector is 
structured into different operations (`read`, `readAll`, `write`, etc.) working on different resource types/API endpoints 
(`assets`, `events`, `contextualization`, etc.). 

Most operations will consume and/or return some data objects. The Connector uses typed objects based on Protobuf 
([https://developers.google.com/protocol-buffers](https://developers.google.com/protocol-buffers)). The structure 
of the data transfer objects follow the structure of the Cognite API objects. 

### Data transfer objects (resource types)

The Beam Connector uses typed data transfer objects to represent the various resource types (`Asset`, `Event`, `File`, etc.). 
The data objects are based on [protocol buffers (protobuf)](https://developers.google.com/protocol-buffers). They have a useful 
Java tutorial which can introduce you to the basics: 
[https://developers.google.com/protocol-buffers/docs/javatutorial](https://developers.google.com/protocol-buffers/docs/javatutorial).

Protbuf is a language-nautral way of specifying data objects, the objects are immutable, and it offers fairly good 
serialization. These are useful properties in general, but also specifically valuable for use in distributed computing--which 
is why we chose it for this SDK (the Java SDK is also the foundation for the 
[Apache Beam Connector](https://github.com/cognitedata/cdf-beam-connector-java) --a distributed data processing framework). 

Protobuf objects are based on the builder pattern and hence the data objects have two states: 1) a builder which is the 
state where you primarily set/add values and 2) message/sealed where you read the object's values. All data object types 
has the same structure as their API siblings. That is, the data objects mirror the API resource types' attribute names 
and types. The [API documentation](https://docs.cognite.com/api/v1/) is a very useful reference for the data objects.

The typical usage patterns for the data objects are:
- _Create_ a new object via the builder, `newBuilder()`, then set the attribute values and finally call `build()`.
- _Read_ the data from an existing data object via the `hasValue()` and `getValue()` methods.
- _Change_ an object by first converting it to a builder via `toBuilder()`, then setting new values and finally 
calling `build()`. Please note that this will not change the original data object (it is immutable), but give you a new 
  copy with the changed attribute values.

```java
// Create a new data object via its builder 
Event originalEvent = Event.newBuilder()                                            // Get the builder for the type
        .setExternalId("myEvent:eventA")                                            // Literals have simple "setters"
        .setStartTime(Instant.parse("2021-01-01T01:01:01.00Z").toEpochMilli())
        .setEndTime(Instant.parse("2021-01-11T01:01:01.00Z").toEpochMilli())
        .setDescription("Change equipment")
        .setType("maintenance")
        .setSubtype("workorder")
        .setSource("myPlantMaintenanceSystem")
        .addAssetIds(148978934560L)                                                 // Arrays have additional methods like "add"
        .putMetadata("sourceId", "eventA")                                          // Maps have additional methods like "put"
        .build();                                                                   // Seal the object via "build"

// "Change" an existing object
Event modifiedEvent = originalEvent.toBuilder()                                     // Get a builder based on an existing object
        .setDescription("My new description")                                       // Set new values (or clear fields)
        .build();                                                                   // Build a new data object (seal it)

// Read data objects
if (modifiedEvent.hasExternalId()) {                                                // Check if a field has its value set
    LOG.info("The event external id is: {}", modifiedEvent.getExternalId());        // Get literal values via getters
}

if (modifiedEvent.getAssetsCount() > 0) {
    List<Long> assetIds = modifieldEvent.getAssetIdsList();                         // Access a list as a Java List...
    LOG.info("Asset id at index 0: {}", modifiedEvent.getAssetIds(0));              // ... or get an item from the list directly    
}

if (modifiedEvent.getMetadataCount() > 0) {
    Map<String, String> metadata = modifieldEvent.getMetadataMap();                 // Access a map as a Java Map...
    LOG.info("Metadata sourceId: {}", modifiedEvent.getMetadataOrDefault("sourceId", "myDefault")); // ... or get an item from the map directly    
}
```

### Migrating from Connector <0.9.24

Previous to version 0.9.24 the protobuf objects used a workaround to represent _optional_ attributes. Due to protobuf not 
supporting `optional` as a native feature (in proto3), we employed a workaround where we used wrapper objects for all 
literal values. `String` literals were wrapped in a `StringValue` object, `long` were wrapped in a `Int64Value` object 
and so on. 

The reason for this was that we needed the `hasValue()` methods in order to let the client check if an attribute value 
was actually set--the wrapper objects gave us just that. The disadvantage was that the wrapper objects made the client 
code more verbose, both when setting and getting attribute values:
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

Starting with version 0.9.24, we have switched to using protobuf's very recent introduction of native support for 
`optional`. This allows us to enjoy the benefits of `hasValue()` but without the drawback of the verbosity of the 
wrapper object. We realize that this is a breaking change for your client code--but we believe that the overall 
benefit is worth making this change before locking down the SDK for the v1 release.