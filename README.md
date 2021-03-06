# A "Kafka Consumer"-Channel for Flume

This is a very high throughput channel for Flume that enables use of Flume as a *high-speed* and *reliable* Kafka consumer.


**How fast does it go ?**

It clocks around 360 MB/s with a single Null Sink (fastest sink) attached to it when consuming 1000 byte events from a 1 broker Kafka cluster over a 10GigE connection. With slower sinks it will run as fast as the sink can go. 

In contrast, the Kafka Source when configured with a Memory channel and a Null sink can deliver about 150 MB/s. For reliabile recovery from Flume agent crashes, however, the File channel will be required. With a single disk, File channel can deliver only about 40 MB/s. 



**Why is it reliable ?**

  This channel relies on Kafka's ability to retain consumed events. In case of a failed delivery attempt by the sink or if the Flume process crashes, we can still retreive such "in flight" events from Kafka and deliver them at a later point in time. It provides "at least once" delivery guarantee. 
  Since it does not rely on buffering at all, there will be no data loss even if the hardware on which the Flume agent is running fails completely.


**What makes it so fast and efficient ?**

This channel differs from the traditional Flume channels in two ways:
  - It does not buffer any events. It enables the Flume sinks to pull their data from Kafka.
  - Flume Sources cannot be used to feed data to it as it procures data from Kafka directly.

This essentially means that the sinks acquire data directly from Kafka without intermediaries. A traditional Flume configuration to pull data from Kafka, a Kafka source would first pull the events from Kafka broker and then buffer it into the chosen Flume channel (like File channel) and finally a sink(such as HDFS or Hive sink), would then drain events from this channel. The efficiency is gained by side stepping the additional steps and synchronization between sources, channels and sinks. Combined with lack of bufferring this translates to reduced consumption of memory, disk and CPU.


**Flume as a universal Kafka consumer**

  Any of the standard Flume sinks can be used with this channel. Since Flume comes with a wide variety of well tested sinks that  deliver to a many different destinations, this channel allows Flume to be used as an out-of-the-box  "universal" Kafka consumer.


**What are the limitations ?**

- It can only pull events from Kafka. Other Flume channels work with any of Flume Sources. This channel does not accept any sources. It is desgined specifically for consuming data from Kafka.
- Flume sources provide some flexiblity like discarding events or adding additional headers to incoming events via interceptors. Currently there is no support for interceptors.
- Currently this is in early beta. Works with only 1 sink. Needs some fixes to allow multiple sinks. It is not well tested as yet, so most likely it has bugs and missing features.

**How is it different than the Kafka channel that is already included in Flume ?**

  The Kafka channel that is built into Flume serves a different use case. It uses a Kafka cluster as a reliable external distributed buffer instead of bufferring on local disks or local memory. It is not intended to make Flume behave as a Kafka consumer. 
   

**Sample Flume Config**

```
a1.channels = c1

# Configure the channel
a1.channels.c1.type = org.apache.flume.channel.KChannel
a1.channels.c1.zookeeperConnect = xyz.domain.com:2181
a1.channels.c1.transactionCapacity = 1000
a1.channels.c1.topic = parts_4_1k
a1.channels.c1.groupId = group1
a1.channels.c1.sink.count = 1

# Set properties for underlying Kafka consumer API
a1.channels.c1.kafka.auto.offset.reset = smallest
a1.channels.c1.kafka.consumer.timeout.ms = 100
a1.channels.c1.kafka.fetch.message.max.bytes = 2097152
a1.channels.c1.kafka.socket.receive.buffer.bytes = 2097152
a1.channels.c1.kafka.num.consumer.fetchers = 1
a1.channels.c1.kafka.auto.commit.enable = false


#... configire a sink .. but no source 

```
