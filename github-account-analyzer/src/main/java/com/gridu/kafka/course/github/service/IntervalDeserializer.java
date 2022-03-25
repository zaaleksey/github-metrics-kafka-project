package com.gridu.kafka.course.github.service;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Deserializer of interval string values. */
public class IntervalDeserializer {

  public enum IntervalUnit {
    HOURS, DAYS, WEEKS
  }

  private static final Pattern DURATION_IN_TEXT_FORMAT = Pattern.compile("^([+\\-]?\\d+)([a-zA-Z]{1,2})$");

  private final Map<String, IntervalUnit> intervalUnits;

  public IntervalDeserializer() {
    intervalUnits = new HashMap<>();
    intervalUnits.put("h", IntervalUnit.HOURS);
    intervalUnits.put("d", IntervalUnit.DAYS);
    intervalUnits.put("w", IntervalUnit.WEEKS);
  }

  /**
   * Deserializes interval and subtracts it from `now` date/time, returns the result as LocalDateTime.
   *
   * @param interval interval in a string form in format like "1d", "3w" etc.
   * @return starting date counted from `now` with the provided interval
   */
  public LocalDateTime startingDateTime(String interval) {
    Matcher matcher = DURATION_IN_TEXT_FORMAT.matcher(interval);

    assertTrue(matcher.matches(),
        "Interval is malformed, it should be in format <number><unit>, e.g. 3h, 1d, 5w");

    int amount = Integer.parseInt(matcher.group(1));
    IntervalUnit unit = intervalUnits.get(matcher.group(2).toLowerCase());
    assertNotNull(unit, "Unsupported unit, only hours (h), days (d) and weeks (w) are supported");

    LocalDateTime result;
    switch (unit) {
      case HOURS:
        result = LocalDateTime.now().minusHours(amount);
        break;
      case DAYS:
        result = LocalDateTime.now().minusDays(amount);
        break;
      case WEEKS:
        result = LocalDateTime.now().minusWeeks(amount);
        break;
      default:
        result = null;
    }

    return result;
  }

}
