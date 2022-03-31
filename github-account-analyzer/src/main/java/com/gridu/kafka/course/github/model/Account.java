package com.gridu.kafka.course.github.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

/** The representation of account record read from the file. */
@Getter
@ToString
@EqualsAndHashCode
@NoArgsConstructor
public class Account {
  /** GitHub account login. */
  private String account;
  /** Can be hours, days or weeks, and should have a format like "1d", "2w", "3h". */
  private String interval;

  public Account setAccount(String account) {
    this.account = account;
    return this;
  }

  public Account setInterval(String interval) {
    this.interval = interval;
    return this;
  }

}

