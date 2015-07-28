# A "Kafka Consumer"-Channel for Flume

This is a very high throughput channel for Flume that enables use of Flume as a *universal*, *efficient*, *reliable* and *simple* Kafka consumer.

**What makes it universal ?**

  Any of the standard Flume sinks can be used with this channel. Since Flume comes with a wide variety of well tested sinks that  deliver to a many different destinations, this channel allows Flume to be used as an out-of-the-box  "universal" Kafka consumer.

**What makes it so efficient ?**

This channel differs from the traditional Flume channels in two ways:
  - Flume Sources cannot be used to feed data to it. It procures data from Kafka.
  - It does not buffer any events. It enables the Flume sinks to pull their data from Kafka.

This essentially means that the sinks acquire data directly from Kafka without intermediaries. A traditional Flume configuration to pull data from Kafka, a Kafka source would first pull the events from Kafka broker and then buffer it into the chosen Flume channel (like File channel) and finally a sink, such as HDFS sink or Hive sink, would then drain events from this channel. The efficiency is gained by side stepping the additional steps and synchronization issues involved.


**Why is it reliable ?**

  This channel relies on Kafka's ability to retain consumed events. In case of a failed delivery attempt by the sink or if the Flume process crashes, we can still retreive such "in flight" events from Kafka and deliver them at a later point in time. It provides "at least once" delivery guarantee.


**Why is it simple ?**

 It simplifies the end-user configuration as no sources are required. Similarly there is no need to tweak the channel for performance. The only tweaks needed are on the sink configuration.
 

**What are the limitations ?**

- It can only pull events from Kafka. Other Flume channels work with any of Flume Sources. This channel does not accept any sources. It is desgined specifically for consuming data from Kafka.
- Flume sources provide some flexiblity like dropping events or adding additional headers to incoming events via interceptors. Currently there is no support for interceptors.


**How is it different than the Kafka channel that is already included in Flume ?**

  The Kafka channel that is built into Flume serves a different use case. It is not specifically designed to make Flume behave as a Kafka consumer. It provides an alternative to the existing channels  which buffer locally. Events enter the Flume process via the source, get pushed to a Kafka cluster by the Kafka channel, then come back to the Flume process when the sink is ready to drain. In essence, it uses a Kafka cluster as a reliable external distributed buffer instead of bufferring on local disks or local memory.

**Why not use the Flume's Kafka source with a File or Memory Channel instead ?**

   It serves the same use case, but Kafka Consumer channel provides much better throughput. Memory channel will provide decent throughput but has limited bufferring ability and not tolerant to process crashing. File channel will give fault tolerance but the throughput reduces significantly.  Kafka Consumer channel will also require less hardware resources in terms of memory, disk and CPU.
   
