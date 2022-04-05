package streams.transformer;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

/**
 * Calculates top five committers.
 * Produces string value with the top five committers,
 * e.g. "top5_committers: author_top_1 - 10, author_top_2 - 5".
 */
@Slf4j
public class TopFiveCommittersTransformer implements Transformer<String, Long, KeyValue<String, String>> {

  private final String storeName;
  private KeyValueStore<String, ValueAndTimestamp> store;

  public TopFiveCommittersTransformer(String storeName) {
    this.storeName = storeName;
  }

  @Override
  public void init(ProcessorContext processorContext) {
    store = processorContext.getStateStore(storeName);
  }

  @Override
  public KeyValue<String, String> transform(String key, Long value) {
    List<KeyValue<String, ValueAndTimestamp>> authors = new ArrayList<>();
    store.all().forEachRemaining(authors::add);

    authors.sort(Comparator.comparing(o ->
        ((KeyValue<String, ValueAndTimestamp<Long>>) o).value.value()).reversed());

    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 5 && i < authors.size(); i++) {
      String author = authors.get(i).key;
      var number_of_commits = authors.get(i).value.value();
      sb.append(author)
          .append(" (")
          .append(number_of_commits)
          .append(")");

      if (i != 4 && i != authors.size() - 1) {
        sb.append(", ");
      }
    }
    log.info("top5-committers: " + sb);

    return new KeyValue<>("top5-committers", "top5_committers: " + sb);
  }

  @Override
  public void close() {}

}
