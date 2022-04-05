package streams;

import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import streams.transformer.DeduplicateByKeyTransformer;
import streams.transformer.TopFiveCommittersTransformer;
import utils.CommitHelper;

/**
 * Calculates top five of committers by the amount of distinct commits each one has.
 * Produces string value with the top five committers,
 * e.g. "top5_committers: author_top_1 (10), author_top_2 (5)".
 */
public class TopFiveCommittersMetrics extends MetricsStream {

  private static final String DEDUPLICATE_COMMITS_STORE = "top-five-committers-counter";
  private static final String COMMITS_BY_AUTHOR_STORE = "CommitsByAuthor";

  public TopFiveCommittersMetrics(Properties properties, String inputTopic, String outputTopic) {
    super(properties, inputTopic, outputTopic);
    this.properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "commits-metrics-top-five-committers");
  }

  @Override
  protected Topology createTopology() {
    StreamsBuilder streamsBuilder = new StreamsBuilder();

    StoreBuilder<KeyValueStore<String, String>> keyValueStoreBuilder = Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(DEDUPLICATE_COMMITS_STORE),
        Serdes.String(), Serdes.String());
    streamsBuilder.addStateStore(keyValueStoreBuilder);

    KStream<String, String> topCommitters = streamsBuilder
        .stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
        .transform(() -> new DeduplicateByKeyTransformer(DEDUPLICATE_COMMITS_STORE), DEDUPLICATE_COMMITS_STORE)
        .selectKey(CommitHelper::getAuthorOfCommit)
        .groupByKey()
        .count(Materialized.as(COMMITS_BY_AUTHOR_STORE))
        .toStream()
        .transform(() -> new TopFiveCommittersTransformer(COMMITS_BY_AUTHOR_STORE), COMMITS_BY_AUTHOR_STORE);

    topCommitters.to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

    return streamsBuilder.build();
  }

}
