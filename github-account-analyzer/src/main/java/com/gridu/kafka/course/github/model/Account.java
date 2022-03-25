package com.gridu.kafka.course.github.model;

import lombok.Data;

/** The representation of account record read from the file. */
@Data
public class Account {

  /** GitHub account login. */
  private String account;
  /** Can be hours, days or weeks, and should have a format like "1d", "2w", "3h". */
  private String interval;

}

