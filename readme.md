***How did changing values on the SparkSession property parameters affect the throughput and latency of the data***

Any change in SparkSession parameter will impact throughput and latency.  We can check progress report to see how it is being impacted.
Couple of fields to take a look are 
**numInputRows**:  This indicates number of records processed in a trigger
**processedRowsPerSecond** : This shows rate at which Spark is processing the records


***What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?***

**maxRatePerPartition** : This property indicates number of messages per second at which each Kafka partition will be read by Spark.When this value is set, it will make sure messages are read at a constant rate.  This wil prevent batches from getting overloaded, when there is a sudden surge of messages from Kafka producers.
This also makes sure the very first micr-batch from getting overloaded at the very first run.

**trigger**: Th value we set for trigger option decides how quick a stream reads next set of record. If we do not set any value for this, Spark will try to process next available records ASAP. If we want to control, how often we shour trigger these microbatches, we can use , "Processing Time" option. For example, if you set a trigger interval of “20 seconds” then for every 20 seconds a micro batch will process a batch of records.
When I did not set any option for trigger, records were getting processed at an average of 1 sec. With "processing Time" option, event was getting triggered at every 20 seconds.
