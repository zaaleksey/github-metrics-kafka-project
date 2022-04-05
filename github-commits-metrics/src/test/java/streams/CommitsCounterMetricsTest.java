package streams;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.gridu.kafka.course.github.model.Commit;
import java.time.LocalDateTime;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CommitsCounterMetricsTest {

  private static final String INPUT_TOPIC = "input";
  private static final String OUTPUT_TOPIC = "output";

  private TopologyTestDriver testDriver;
  private TestInputTopic<String, String> inputTopic;
  private TestOutputTopic<String, String> outputTopic;
  private final Serde<String> stringSerde = new Serdes.StringSerde();

  private static final ObjectMapper mapper = new ObjectMapper();

  @BeforeAll
  static void setUpMapper() {
    mapper.registerModule(new JavaTimeModule());
    mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
  }

  @BeforeEach
  void setUp() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    MetricsStream metricsStream = new CommitsCounterMetrics(properties, INPUT_TOPIC, OUTPUT_TOPIC);
    Topology topology = metricsStream.createTopology();

    testDriver = new TopologyTestDriver(topology, properties);
    inputTopic = testDriver.createInputTopic(INPUT_TOPIC, stringSerde.serializer(), stringSerde.serializer());
    outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC, stringSerde.deserializer(), stringSerde.deserializer());
  }

  @AfterEach
  void tearDown() {
    testDriver.close();
  }

  @Test
  void commitsAreCounted() throws JsonProcessingException {
    Commit commit1 = new Commit()
        .setAuthor("author")
        .setLanguage("java")
        .setMessage("message1")
        .setRepository("repository")
        .setSha("sha1")
        .setDateTime(LocalDateTime.now());
    String commit1Json = mapper.writeValueAsString(commit1);
    inputTopic.pipeInput(commit1.getSha(), commit1Json);

    Commit commit2 = new Commit()
        .setAuthor("author")
        .setLanguage("java")
        .setMessage("message2")
        .setRepository("repository")
        .setSha("sha2")
        .setDateTime(LocalDateTime.now());
    String commit2Json = mapper.writeValueAsString(commit2);
    inputTopic.pipeInput(commit2.getSha(), commit2Json);

    assertEquals("total_commits: 1", outputTopic.readValue());
    assertEquals("total_commits: 2", outputTopic.readValue());
    assertTrue(outputTopic.isEmpty());
  }

  @Test
  void checkDeduplicateCommitsCounter() throws JsonProcessingException {
    Commit commit = new Commit()
        .setAuthor("author")
        .setLanguage("java")
        .setMessage("message")
        .setRepository("repository")
        .setSha("sha")
        .setDateTime(LocalDateTime.now());
    String commitJson = mapper.writeValueAsString(commit);

    inputTopic.pipeInput(commit.getSha(), commitJson);
    inputTopic.pipeInput(commit.getSha(), commitJson);

    assertEquals("total_commits: 1", outputTopic.readValue());
    assertTrue(outputTopic.isEmpty());
  }

  @Test
  void closeKafkaStream() {
    MetricsStream metrics =
        new CommitsCounterMetrics(new Properties(), INPUT_TOPIC, OUTPUT_TOPIC);
    metrics.kafkaStreams = mock(KafkaStreams.class);

    metrics.close();
    verify(metrics.kafkaStreams, times(1)).close();
  }

}