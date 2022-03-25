package com.gridu.kafka.course.github.service;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.gridu.kafka.course.github.model.Commit;
import java.time.LocalDateTime;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CommitsProducerTest {

  private static ObjectMapper mapper;

  @Mock
  private KafkaProducer<String, String> kafkaProducer;

  private CommitsProducer commitsProducerUnderTest;

  @BeforeAll
  static void setUpObjectMapper() {
    mapper = new ObjectMapper();
    mapper.registerModule(new JavaTimeModule());
    mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
  }

  @BeforeEach
  void setUpCommitsProducer() {
    commitsProducerUnderTest = new CommitsProducer("localhost:9090", "topic");
    commitsProducerUnderTest.producer = kafkaProducer;
  }

  @Test
  void close() {
    commitsProducerUnderTest.close();
    verify(kafkaProducer, times(1)).flush();
    verify(kafkaProducer, times(1)).close();
  }

  @Test
  void send() throws JsonProcessingException {
    Commit commit = new Commit()
        .setAuthor("author")
        .setDateTime(LocalDateTime.now())
        .setSha("sha")
        .setLanguage("language")
        .setMessage("message")
        .setRepository("repository");

    String commitJson = mapper.writeValueAsString(commit);
    ProducerRecord<String, String> record = new ProducerRecord<>("topic", commit.getSha(), commitJson);
    commitsProducerUnderTest.send(commit);

    verify(kafkaProducer, times(1)).send(record);
  }

}