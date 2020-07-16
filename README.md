## NYC Taxi & Limousine Commission's open data with Spark Streaming 3.0.0

__This is a project where I consume streaming data from Kafka topic A,
transform it and write the results to either other topics or to console. I used both Streaming and Structured Streaming API's.__

This data was gathered from [NYC Taxi Dataflow Codelab](https://github.com/googlecodelabs/cloud-dataflow-nyc-taxi-tycoon).

The available data in topic A is in json format and has the following schema:

- ride_id:string
- point_idx:integer
- latitude:float
- longitude:float
- timestamp:timestamp
- meter_reading:float
- meter_increment:float
- ride_status:string
- passenger_count:integer

There are mainly two ways for working with Streams in Spark:
 - [Spark Streaming](https://spark.apache.org/docs/latest/streaming-programming-guide.html)
 - [Spark Structured Streaming](http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)

In this project both options are used, that's why there are two classes that execute operations on the kafka topic: __KafkaStreaming.scala__ and __KafkaStructuredStreaming.scala__.
Some transformations are done in the first class and more complex transformations that require _Event-Time Windows_ and _Session Windows_ are done with the latter.

## Spark Streaming API

### Counting rides per second

The transformation is done in TaxiOperations.parseDStreamTaxiCountRides().
The output data has the following schema (JSON):

- key:string
- latitude:float
- longitude:float
- ridesCount:integer

Where ridesCount is the total amount of taxi rides that occurred within cells of 500m<sup>2</sup>
across New York, in a window of time of 1 second. Latitude and Longitude are the center of those cells.

This output is going to be useful for displaying a heatmap of the taxi rides in NY.

### Dollar collected per minute

The transformation is done in TaxiOperations.parseDStreamTaxiSumIncrements().
The output data has the following schema (JSON):

- dollar_per_minute:float

dollar_per_minute is the sum of each _meter_increment_ field in every taxi ride during the last 60 seconds, computed each 3 seconds.

## Spark Structured Streaming API

Previous transformation are done in _process-time_ and are not taking into consideration _late arriving data_. Briefly explained, remember that our events contain a timestamp which is created before it is sent over the network to the Kafka Topic.
Then, there is no guarantee that the events are going to arrive ordered by timestamp! After all, the network is unpredictable. It is common to receive events that occured earlier than previously processed events in our spark stream. This is were _event-time_ processing comes into play. It is [clearly explained](http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#window-operations-on-event-time) in the official docs. 

### Dollar collected per minute in Event Time

Check the function TaxiStructuredOperations.toSumIncrementsEventTime() for the implementation.

Previous output was not considering event time, thus it may not lead to accurrate results.
Each taxi ride has a timestamp, which we will consider as the event time. Now we can use accurate time windows and receive data out of order, because Spark Streaming has 
[built-in methods to handle this](http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#window-operations-on-event-time). We can specify a delayThreshold of 60 seconds for late data and window of 60 seconds with a slide duration of 10 seconds.

The output data has the following schema (JSON):

- window: {
  start: string,
  end: string
}
- sum(meter_increment): float

### Busiest pickup locations

Check the function TaxiStructuredOperations.toSessionWindowPerRide() for the implementation.

We can display on a map the busiest pickup locations by considerig only the pickup _latitude_ and _longitude_ of a taxi ride, for as long as it is active.
__Sessions__ are very useful in this case, because they can keep track of a computation until a condition is met (usually a timeout). Thus, we will consider a taxi ride to be active until we do not receive any event of that taxi ride for a certain period of time.
In this example, _ride_id_ is going to be our _session_id_.
 
Here's an example of [mapGroupsWithState](https://github.com/apache/spark/blob/v3.0.0/examples/src/main/scala/org/apache/spark/examples/sql/streaming/StructuredSessionization.scala), which is a key function that allows us to combine different events on a session for calculations.
 
 The output of the data is a dataframe printed to console with the following schema:
 
  __sessionId | latitude | longitude | startTimestamp | durationMs | expired__