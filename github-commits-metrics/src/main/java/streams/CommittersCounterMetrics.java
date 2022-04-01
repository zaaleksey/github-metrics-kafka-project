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
import utils.CommitHelper;

/**
 * Counts total number of distinct committers.
 * Produces string value with the number of committers, e.g. "total_committers: 5".
 */
public class CommittersCounterMetrics extends MetricsStream {

  private static final String DEDUPLICATE_COMMITS_STORE = "committers-counter";

  public CommittersCounterMetrics(Properties properties, String inputTopic, String outputTopic) {
    super(properties, inputTopic, outputTopic);
    this.properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "commits-metrics-committers-counter");
  }

  @Override
  protected Topology createTopology() {
    StreamsBuilder streamsBuilder = new StreamsBuilder();

    StoreBuilder<KeyValueStore<String, String>> keyValueStoreBuilder =
        Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(DEDUPLICATE_COMMITS_STORE),
            Serdes.String(), Serdes.String());
    streamsBuilder.addStateStore(keyValueStoreBuilder);

    KTable<String, String> committersCount = streamsBuilder
        .stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
        .mapValues(CommitHelper::getAuthorOfCommit)
        .selectKey((key, value) -> value)
        .transform(() -> new DeduplicateByKeyTransformer(DEDUPLICATE_COMMITS_STORE), DEDUPLICATE_COMMITS_STORE)
        .selectKey((key, value) -> "total_committers")
        .groupByKey()
        .count()
        .mapValues(value -> "total_committers: " + value);

    committersCount.toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

    return streamsBuilder.build();
  }

}
