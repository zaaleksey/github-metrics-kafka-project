package streams.transformer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

/** Deduplicate records by their key - if a message was already processed before, it will be filtered out. */
@Slf4j
public class DeduplicateByKeyTransformer implements Transformer<String, String, KeyValue<String, String>> {

  private final String storeName;
  private KeyValueStore<String, String> store;

  public DeduplicateByKeyTransformer(String storeName) {
    this.storeName = storeName;
  }


  @Override
  public void init(ProcessorContext processorContext) {
    store = processorContext.getStateStore(storeName);
  }

  @Override
  public KeyValue<String, String> transform(String key, String value) {
    String res = store.putIfAbsent(key, value);
    log.info("lookup result for key " + key + ": " + res);
    if (res != null) {
      return null;
    }

    return new KeyValue<>(key, value);
  }

  @Override
  public void close() {}

}
