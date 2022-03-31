package com.gridu.kafka.course.github.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.gridu.kafka.course.github.model.Commit;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Wrapper around KafkaProducer.
 * Produces GitHub commits objects.
 */
@Slf4j
public class CommitsProducer {

  private final String topic;
  private final ObjectMapper mapper;
  KafkaProducer<String, String> producer;

  public CommitsProducer(String bootstrapServer, String topic) {
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");

    this.topic = topic;
    mapper = new ObjectMapper();
    mapper.registerModule(new JavaTimeModule());
    mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

    producer = new KafkaProducer<>(properties);
  }

  /** Flushes and closes KafkaProducer. */
  public void close() {
    producer.flush();
    producer.close();
  }

  /**
   * Send commit info into kafka topic.
   *
   * @param commit commit ti send to kafka
   */
  public void send(Commit commit) {
    log.info("Pushing commit into kafka: " + commit);

    String commitJson = null;
    try {
      commitJson = mapper.writeValueAsString(commit);
    } catch (JsonProcessingException e) {
      log.warn("Can't write commit as json string", e);
    }

    if (commitJson != null) {
      ProducerRecord<String, String> record = new ProducerRecord<>(topic, commit.getSha(), commitJson);
      producer.send(record);
    }
  }

}
