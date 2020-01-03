package com.northwinds.streamshift;

import io.dropwizard.lifecycle.Managed;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.cloudevents.v1.CloudEventBuilder;
import io.cloudevents.kafka.CloudEventsKafkaProducer;
import io.cloudevents.v1.CloudEventImpl;
import io.cloudevents.v1.AttributesImpl;
import io.cloudevents.v1.kafka.Marshallers;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.Future;

public class StreamshiftProducer implements Managed {
  private static final Logger LOG = LoggerFactory.getLogger(StreamshiftProducer.class);

  private final KafkaConfig config;
  
  private CloudEventsKafkaProducer<String, AttributesImpl, String> ceProducer;

  public StreamshiftProducer(KafkaConfig config) {
    this.config = config;
  }

  public void start() throws Exception {
    LOG.info("starting");
    Properties properties = config.getProperties();
    properties.put(ProducerConfig.ACKS_CONFIG, "all");
    properties.put(ProducerConfig.RETRIES_CONFIG, 0);
    properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
    properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
    properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    //producer = new KafkaProducer<>(properties);
    ceProducer = new CloudEventsKafkaProducer<>(properties, Marshallers.binary());
    LOG.info("started");
  }

  public Future<RecordMetadata> send(String message) {
    LOG.info("Building CloudEvent");
    // Build an event
    CloudEventImpl<String> ce =
    CloudEventBuilder.<String>builder()
      .withId("x10")
      .withSource(URI.create("/streamshift"))
      .withType("change-data-capture")
      .withDataContentType("application/json")
      .withData(message)
      .build();
      LOG.info("Sending CloudEvent");
    // Produce the event
    return ceProducer.send(new ProducerRecord<>(config.getTopic(), ce));
    //return producer.send(new ProducerRecord<>(config.getTopic(), message, message));
  }

  public void stop() throws Exception {
    LOG.info("stopping");
    CloudEventsKafkaProducer<String, AttributesImpl, String> ceProducer = this.ceProducer;
    this.ceProducer = null;
    LOG.info("closing producer");
    ceProducer.close();
    LOG.info("stopped");
  }
}
