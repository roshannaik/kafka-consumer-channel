/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.github.roshan;


import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.channel.BasicChannelSemantics;
import org.apache.flume.channel.BasicTransactionSemantics;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.ChannelCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KonsumerChannel extends BasicChannelSemantics {

  private static Logger log = LoggerFactory.getLogger(KonsumerChannel.class);
  private static final Integer defaultTransCapacity = 100;
  private static final double byteCapacitySlotSize = 100;

  private static final Integer defaultKeepAlive = 3;
  private static final String defaultKafkaAutoCommitEnabled = "false";
  private boolean kafkaAutoCommitEnabled;

  private ConsumerConnector consumer;


  private volatile Integer transCapacity;
  private volatile int keepAlive;
  private String topic;
  private ChannelCounter channelCounter;
  private Properties kafkaProps;
  private List<KafkaStream<byte[], byte[]>> streamList;
  int currentStreamIndex = 0;
  Integer sinkCount =0 ;
  boolean first = true;

  public KonsumerChannel() {
    super();
  }

  /**
   * Read parameters from context
   * <li>capacity = type long that defines the total number of events allowed at one time in the queue.
   * <li>transactionCapacity = type long that defines the total number of events allowed in one transaction.
   * <li>byteCapacity = type long that defines the max number of bytes used for events in the queue.
   * <li>byteCapacityBufferPercentage = type int that defines the percent of buffer between byteCapacity and the estimated event size.
   * <li>keep-alive = type int that defines the number of second to wait for a queue permit
   */
  @Override
  public void configure(Context context) {

    try {
      transCapacity = context.getInteger("transactionCapacity", defaultTransCapacity);
    } catch(NumberFormatException e) {
      transCapacity = defaultTransCapacity;
      log.warn("Invalid transaction capacity specified, initializing channel"
              + " to default capacity of {}", defaultTransCapacity);
    }

    if (transCapacity <= 0) {
      transCapacity = defaultTransCapacity;
      log.warn("Invalid transation capacity specified, initializing channel"
              + " to default capacity of {}", defaultTransCapacity);
    }

    try {
      keepAlive = context.getInteger("keep-alive", defaultKeepAlive);
    } catch(NumberFormatException e) {
      keepAlive = defaultKeepAlive;
    }

    topic = context.getString("topic");

    if(topic == null) {
      throw new ConfigurationException("Kafka topic name must be specified.");
    }

    sinkCount = context.getInteger("sink.count");
    if(sinkCount == null) {
      throw new ConfigurationException("sink.count must be specified.");
    }

    kafkaProps = getKafkaProperties(context);

    kafkaAutoCommitEnabled = Boolean.parseBoolean(kafkaProps.getProperty("auto.commit.enable"
            , defaultKafkaAutoCommitEnabled));

    if (channelCounter == null) {
      channelCounter = new ChannelCounter(getName());
    }
  }

  @Override
  public synchronized void start() {

    try {
      //initialize a consumer. This creates the connection to ZooKeeper
      consumer = getConsumer(kafkaProps);
    } catch (Exception e) {
      throw new FlumeException("Unable to create Kafka Consumer. " +
              "Check whether the ZooKeeper server is up and that the " +
              "ready to connect with.", e);
    }

    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(topic, sinkCount);

    // Get the message iterator for our topic
    // Note that this succeeds even if the topic doesn't exist
    // in that case we simply get no messages for the topic
    // Also note that currently we only support a single topic
    try {
      Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap =
              consumer.createMessageStreams(topicCountMap);
      streamList = consumerMap.get(topic);
    } catch (Exception e) {
      throw new FlumeException("Unable to get Kafka Consumer stream", e);
    }
    log.info("Channel {} started.", getName());

    channelCounter.start();
    super.start();
  }

  @Override
  public synchronized void stop() {
    channelCounter.stop();

    if (consumer != null) {
      // This syncs offsets to ZooKeeper to avoid re-reading the messages
      consumer.shutdown();
    }

    log.info("Channel {} stopped. Metrics: {}", getName(), channelCounter);

    super.stop();
  }

  @Override
  synchronized protected BasicTransactionSemantics createTransaction() {
    if (currentStreamIndex >= streamList.size() ) {
      currentStreamIndex = 0;
    }

    KafkaStream<byte[], byte[]> stream = streamList.get(currentStreamIndex);
    ++currentStreamIndex;

    return new KTransaction(transCapacity, channelCounter, stream);
  }

  private long estimateEventSize(Event event)
  {
    byte[] body = event.getBody();
    if(body != null && body.length != 0) {
      return body.length;
    }
    //Each event occupies at least 1 slot, so return 1.
    return 1;
  }

  public static ConsumerConnector getConsumer(Properties kafkaProps) {
    return  Consumer.createJavaConsumerConnector(new ConsumerConfig(kafkaProps));
  }

  /**
   * Add all configuration parameters starting with "kafka"
   * to consumer properties
   */
  private static void setKafkaProps(Context context,Properties kafkaProps) {

    Map<String,String> kafkaProperties =
            context.getSubProperties(Constants.PROPERTY_PREFIX);

    for (Map.Entry<String,String> prop : kafkaProperties.entrySet()) {

      kafkaProps.put(prop.getKey(), prop.getValue());
      if (log.isDebugEnabled()) {
        log.debug("Reading a Kafka Producer Property: key: "
                + prop.getKey() + ", value: " + prop.getValue());
      }
    }
  }

  public static Properties getKafkaProperties(Context context) {
    log.info("context={}",context.toString());
    Properties props =  generateDefaultKafkaProps();
    setKafkaProps(context,props);
    addDocumentedKafkaProps(context,props);
    return props;
  }

  private static void addDocumentedKafkaProps(Context context,
                                              Properties kafkaProps)
          throws ConfigurationException {
    String zookeeperConnect = context.getString(
            Constants.ZOOKEEPER_CONNECT_FLUME);
    if (zookeeperConnect == null) {
      throw new ConfigurationException("ZookeeperConnect must contain " +
              "at least one ZooKeeper server");
    }
    kafkaProps.put(Constants.ZOOKEEPER_CONNECT, zookeeperConnect);

    String groupID = context.getString(Constants.GROUP_ID_FLUME);

    if (groupID != null ) {
      kafkaProps.put(Constants.GROUP_ID, groupID);
    }
  }
  /**
   * Generate consumer properties object with some defaults
   * @return
   */
  private static Properties generateDefaultKafkaProps() {
    Properties props = new Properties();
    props.put(Constants.AUTO_COMMIT_ENABLED,
            Constants.DEFAULT_AUTO_COMMIT);
    props.put(Constants.CONSUMER_TIMEOUT,
            Constants.DEFAULT_CONSUMER_TIMEOUT);
    props.put(Constants.GROUP_ID,
            Constants.DEFAULT_GROUP_ID);
    return props;
  }

  private class KTransaction extends BasicTransactionSemantics {
    private final ChannelCounter channelCounter;
    private KafkaStream<byte[], byte[]> stream;
    private ConsumerIterator<byte[], byte[]> streamItr;
    private int takeByteCounter = 0;
    private int takes = 0;

    public KTransaction(int transCapacity, ChannelCounter counter, KafkaStream<byte[], byte[]> stream) {
      this.channelCounter = counter;
      this.stream = stream;
      this.streamItr = stream.iterator();
    }

    @Override
    protected void doPut(Event event) throws InterruptedException {
      throw new ChannelException(
              "Sources cannot add events to this channel. It retrieves data directly from Kafka");
    }

    @Override
    protected Event doTake() throws InterruptedException {
      if(first) {
        first = false;
        log.error(" ************>>>   Starting now");
      }
      channelCounter.incrementEventTakeAttemptCount();
      ++takes;

      Map<String, String> headers = new HashMap<String, String>(4);
      headers.put(Constants.TOPIC, topic);

      if (! hasNext(streamItr) )
        return null;

      MessageAndMetadata<byte[], byte[]> messageAndMetadata = streamItr.next();
      byte[] kafkaMessage = messageAndMetadata.message();
      byte[] kafkaKey = messageAndMetadata.key();
      if (kafkaKey != null) {
        headers.put(Constants.KEY, new String(kafkaKey));
      }
      Event event = EventBuilder.withBody(kafkaMessage, headers);

      int eventByteSize = (int)Math.ceil(estimateEventSize(event)/byteCapacitySlotSize);
      takeByteCounter += eventByteSize;

      return event;
    }


    private boolean hasNext( ConsumerIterator<byte[], byte[]> itr) {
      try {
        if( itr.hasNext() )
          return true;
        return  false;
      } catch (ConsumerTimeoutException e) {
        log.info(" *** TIMEOUT " +  channelCounter.getEventTakeSuccessCount());
        try {
          if( itr.hasNext() )
            return true;
          return  false;
        } catch (ConsumerTimeoutException e2) {
          log.info(" *** 2nd TIMEOUT " +  channelCounter.getEventTakeSuccessCount());
        }
      }
      return false;
    }

    @Override
    protected void doCommit() throws InterruptedException {
      if (takes > 0) {
        channelCounter.addToEventTakeSuccessCount(takes);
      }

//      if (kafkaAutoCommitEnabled) {
//        consumer.commitOffsets();
//        log.info(" *** event count : " + channelCounter.getEventTakeSuccessCount());
//      }

    }

    @Override
    protected void doRollback() {
    }

  }

  private static class Constants {
    public static final String TOPIC = "topic";
    public static final String KEY = "key";
    public static final String TIMESTAMP = "timestamp";
    public static final String BATCH_SIZE = "batchSize";
    public static final String BATCH_DURATION_MS = "batchDurationMillis";
    public static final String CONSUMER_TIMEOUT = "consumer.timeout.ms";
    public static final String AUTO_COMMIT_ENABLED = "auto.commit.enable";
    public static final String ZOOKEEPER_CONNECT = "zookeeper.connect";
    public static final String ZOOKEEPER_CONNECT_FLUME = "zookeeperConnect";
    public static final String GROUP_ID = "group.id";
    public static final String GROUP_ID_FLUME = "groupId";
    public static final String PROPERTY_PREFIX = "kafka.";


    public static final int DEFAULT_BATCH_SIZE = 1000;
    public static final int DEFAULT_BATCH_DURATION = 1000;
    public static final String DEFAULT_CONSUMER_TIMEOUT = "10";
    public static final String DEFAULT_AUTO_COMMIT =  "false";
    public static final String DEFAULT_GROUP_ID = "flume";

  }
}
