package com.gridu.kafka.course.github.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.time.LocalDateTime;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

class IntervalDeserializerTest {

  private final IntervalDeserializer deserializer = new IntervalDeserializer();

  @Test
  void startingDateTimeForHours() {
    LocalDateTime expected = pureDateTime().minusHours(12);
    LocalDateTime actual = deserializer.startingDateTime("12h");
    assertEquals(expected, actual.withSecond(0).withNano(0));
  }

  @Test
  void startingDateTimeForDays() {
  LocalDateTime expected = pureDateTime().minusDays(5);
  LocalDateTime actual = deserializer.startingDateTime("5d");
  assertEquals(expected, actual.withSecond(0).withNano(0));
  }

  @Test
  void startingDateTimeForWeeks() {
    LocalDateTime expected = pureDateTime().minusWeeks(1);
    LocalDateTime actual = deserializer.startingDateTime("1w");
    assertEquals(expected, actual.withSecond(0).withNano(0));
  }

  @Test
  void startingDateTimeForUpperCaseLetter() {
    LocalDateTime expected = pureDateTime().minusWeeks(1);
    LocalDateTime actual = deserializer.startingDateTime("1W");
    assertEquals(expected, actual.withSecond(0).withNano(0));
  }

  @Test
  void countStartingDateTimeIncorrectFormatThrowsException() {
    assertThrows(AssertionFailedError.class, () -> deserializer.startingDateTime("one-day"));
  }

  @Test
  void countStartingDateTimeUnsupportedCronoUnitThrowsException() {
    assertThrows(AssertionFailedError.class, () -> deserializer.startingDateTime("120m"));
  }

  private LocalDateTime pureDateTime() {
    return LocalDateTime.now()
        .withSecond(0)
        .withNano(0);
  }

}