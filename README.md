# commit-log

### Project to implement a write-ahead log

The goal of this project is to implement a write-ahead
log system (i.e. an append-only data structure)
that resembles data structures that are used in
popular tools like distributed message queues
(e.g Kafka) and storage engines (e.g. PostgresSQL)

#### Abstractions

I model the problem space through five major 
abstractions

- Record - the data stored in our Log system
- Store - the file that contains Records
- Index - the file that contains index entries
- Segment - Links a Store and Index object together
- Log - Links multiple Segments together

