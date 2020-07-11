## Spark Streaming 3.0.0 with Kafka 2.5

__This is a small project where I consume streaming data from Kafka topic A,
transform it and write the result to another topic B.__

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

This data was gathered from [NYC Taxi Dataflow Codelab](https://github.com/googlecodelabs/cloud-dataflow-nyc-taxi-tycoon).

### Counting rides per second

The output data has the following schema (JSON):

- key:string
- latitude:float
- longitude:float
- ridesCount:integer

Where ridesCount is the total amount of taxi rides that occurred within cells of 500m<sup>2</sup>
across New York, in a window of time of 1 second. Latitude and Longitude are the center of those cells.

This output is going to be useful for displaying a heatmap of the taxi rides in NY.

### Dollar collected per minute

The output data has the following schema (JSON):

- dollar_per_minute:float

dollar_per_minute is the sum of each _meter_increment_ field in every taxi ride during the last 60 seconds, computed each 3 seconds.