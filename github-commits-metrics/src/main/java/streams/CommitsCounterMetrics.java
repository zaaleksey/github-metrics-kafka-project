package streams;

import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import streams.transformer.DeduplicateByKeyTransformer;

/**
 * Counts total number of commits.
 * Produces string value with the number of commits, e.g. "total_commits: 5".
 */
public class CommitsCounterMetrics extends MetricsStream {

  private static final String DEDUPLICATE_COMMITS_STORE = "commits-counter";

  public CommitsCounterMetrics(Properties properties, String inputTopic, String outputTopic) {
    super(properties, inputTopic, outputTopic);
    this.properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "commits-metrics-commits-counter");
  }

  @Override
  protected Topology createTopology() {
    StreamsBuilder streamsBuilder = new StreamsBuilder();

    StoreBuilder<KeyValueStore<String, String>> keyValueStoreBuilder =
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(DEDUPLICATE_COMMITS_STORE), Serdes.String(), Serdes.String());
    streamsBuilder.addStateStore(keyValueStoreBuilder);

    KTable<String, String> totalCommitsNumber = streamsBuilder
        .stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
        .transform(() -> new DeduplicateByKeyTransformer(DEDUPLICATE_COMMITS_STORE), DEDUPLICATE_COMMITS_STORE)
        .selectKey((key, value) -> "total-commits-number")
        .groupByKey()
        .count()
        .mapValues(value -> "total_commits: " + value);

    totalCommitsNumber.toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

    return streamsBuilder.build();
  }

}
