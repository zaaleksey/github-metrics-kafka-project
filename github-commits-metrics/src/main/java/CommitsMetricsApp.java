import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import streams.CommitsCounterMetrics;
import streams.CommittersCounterMetrics;
import streams.LanguagesCounterMetrics;
import streams.MetricsStream;
import streams.TopFiveCommittersMetrics;

@Slf4j
public class CommitsMetricsApp {

  private static final String INPUT_TOPIC = "github-commits";
  private static final String TOTAL_COMMITS_TOPIC = "github-metrics-total-commits";
  private static final String TOTAL_COMMITTERS_TOPIC = "github-metrics-total-committers";
  private static final String TOP_COMMITTERS_TOPIC = "github-metrics-top-committers";
  private static final String LANGUAGES_TOPIC = "github-metrics-languages";
  private final String bootstrapServer;
  private final List<MetricsStream> metricsStreams;

  public CommitsMetricsApp(String bootstrapServer) {
    this.bootstrapServer = bootstrapServer;

    metricsStreams = new ArrayList<>();
  }

  public void run() {
    MetricsStream totalCommitsStream = new CommitsCounterMetrics(getAppProperties(bootstrapServer),
        INPUT_TOPIC, TOTAL_COMMITS_TOPIC);
    totalCommitsStream.start();
    log.info("Total commits counter stream is launched");
    metricsStreams.add(totalCommitsStream);

    MetricsStream totalCommittersStream = new CommittersCounterMetrics(getAppProperties(bootstrapServer),
        INPUT_TOPIC, TOTAL_COMMITTERS_TOPIC);
    totalCommittersStream.start();
    log.info("Total committers counter stream is launched");
    metricsStreams.add(totalCommittersStream);

    MetricsStream topCommittersStream = new TopFiveCommittersMetrics(getAppProperties(bootstrapServer),
        INPUT_TOPIC, TOP_COMMITTERS_TOPIC);
    topCommittersStream.start();
    log.info("Top five committers counter stream is launched");
    metricsStreams.add(topCommittersStream);

    MetricsStream languagesStream = new LanguagesCounterMetrics(getAppProperties(bootstrapServer),
        INPUT_TOPIC, LANGUAGES_TOPIC);
    languagesStream.start();
    log.info("Languages counter stream is launched");
    metricsStreams.add(languagesStream);

    Runtime.getRuntime().addShutdownHook(new Thread(() -> metricsStreams.forEach(MetricsStream::close)));
  }

  private Properties getAppProperties(String bootstrapServer) {
    Properties properties = new Properties();
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
    return properties;
  }

  public static void main(String[] args) {
    String bootstrapServer = "localhost:9092,localhost:9093,localhost:9094";
    CommitsMetricsApp app = new CommitsMetricsApp(bootstrapServer);
    app.run();
  }

}
