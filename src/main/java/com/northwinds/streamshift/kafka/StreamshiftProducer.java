package com.northwinds.streamshift;

import io.dropwizard.lifecycle.Managed;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.cloudevents.v1.CloudEventBuilder;
import io.cloudevents.v1.CloudEventImpl;
import io.cloudevents.extensions.ExtensionFormat;
import io.cloudevents.json.Json;
import io.cloudevents.extensions.DistributedTracingExtension;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.Future;

public class StreamshiftProducer implements Managed {
  private static final Logger LOG = LoggerFactory.getLogger(StreamshiftProducer.class);

  private final KafkaConfig config;
  
  private Producer<String, String> producer;

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

    producer = new KafkaProducer<>(properties);
    LOG.info("started");
  }

  public Future<RecordMetadata> send(String message) {
    LOG.info("Building CloudEvent");
    // Build an event
    // given
    final String eventId = UUID.randomUUID().toString();
    final URI src = URI.create("/streamshift");
    final String eventType = "My.Cloud.Event.Type";
    final MyCustomEvent payload = message;

    // add trace extension usin the in-memory format
    final DistributedTracingExtension dt = new DistributedTracingExtension();
    dt.setTraceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01");
    dt.setTracestate("rojo=00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01");

    final ExtensionFormat tracing = new DistributedTracingExtension.Format(dt);

    // passing in the given attributes
    final CloudEventImpl<MyCustomEvent> cloudEvent =
      CloudEventBuilder.<MyCustomEvent>builder()
        .withType(eventType)
        .withId(eventId)
        .withSource(src)
        .withData(payload)
        .withExtension(tracing)
        .build();

    // marshalling as json
    final String json = Json.encode(cloudEvent);
    LOG.info("Sending CloudEvent");
    
    // Produce the event
    return ceProducer.send(new ProducerRecord<>(config.getTopic(), json));
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
