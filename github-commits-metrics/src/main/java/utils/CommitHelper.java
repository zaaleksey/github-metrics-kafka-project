package utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.gridu.kafka.course.github.model.Commit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CommitHelper {

  private static final ObjectMapper mapper = new ObjectMapper();

  static {
    mapper.registerModule(new JavaTimeModule());
    mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
  }

  public static String getAuthorOfCommit(String key, String value) {
    Commit commit = null;
    try {
      commit = mapper.readValue(value, Commit.class);
    } catch (JsonProcessingException e) {
      log.warn("Can't read the value - data may be malformed", e);
    }
    return commit != null ? commit.getAuthor() : null;
  }

  public static String getLanguageOfCommit(String key, String value) {
    Commit commit = null;
    try {
      commit = mapper.readValue(value, Commit.class);
    } catch (JsonProcessingException e) {
      log.warn("Can't read the value - data may be malformed", e);
    }
    return commit != null ? commit.getLanguage() : null;
  }

}
