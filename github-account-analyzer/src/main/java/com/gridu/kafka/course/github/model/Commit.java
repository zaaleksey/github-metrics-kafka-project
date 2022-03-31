package com.gridu.kafka.course.github.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.time.LocalDateTime;

/** The representation of commit record. */
@Getter
@ToString
@EqualsAndHashCode
@NoArgsConstructor
public class Commit {
  /** GitHub login of the author of the commit. */
  private String author;
  /** Date and time when the commit was created. */
  private LocalDateTime localDateTime;
  /** Hash of the commit. */
  private String sha;
  /** Programming language of the commit. */
  private String language;
  /** Message of the commit. */
  private String message;
  /** The full name of the commits repository. */
  private String repository;

  public Commit setAuthor(String author) {
    this.author = author;
    return this;
  }

  public Commit setDateTime(LocalDateTime localDateTime) {
    this.localDateTime = localDateTime;
    return this;
  }

  public Commit setSha(String sha) {
    this.sha = sha;
    return this;
  }

  public Commit setLanguage(String language) {
    this.language = language;
    return this;
  }

  public Commit setMessage(String message) {
    this.message = message;
    return this;
  }

  public Commit setRepository(String repository) {
    this.repository = repository;
    return this;
  }

}
