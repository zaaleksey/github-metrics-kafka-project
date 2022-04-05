package streams;

import java.util.Properties;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

/** Simple wrapper for {@link KafkaStreams}. */
public abstract class MetricsStream {

  protected KafkaStreams kafkaStreams;
  protected Properties properties;
  protected String inputTopic;
  protected String outputTopic;

  public MetricsStream(Properties properties, String inputTopic, String outputTopic) {
    this.properties = properties;
    this.inputTopic = inputTopic;
    this.outputTopic = outputTopic;
  }

  /** Proxies the call to KafkaStreams `start` method. */
  public void start() {
    kafkaStreams = new KafkaStreams(createTopology(), properties);
    kafkaStreams.start();
  }

  /** Proxies the call to KafkaStreams `close` method. */
  public void close() {
    kafkaStreams.close();
  }

  /** @return topology for KafkaStreams. */
  protected abstract Topology createTopology();

}