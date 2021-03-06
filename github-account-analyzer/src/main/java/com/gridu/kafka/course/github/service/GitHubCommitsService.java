package com.gridu.kafka.course.github.service;

import com.gridu.kafka.course.github.model.Commit;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.kohsuke.github.GHCommit;
import org.kohsuke.github.GitHub;
import org.kohsuke.github.GitHubBuilder;

/** Wrapper for GitHub API. */
@Slf4j
public class GitHubCommitsService {

  GitHub gitHub;

  /** Constructs the service. The access token must be contained in properties file ~/.github. */
  public GitHubCommitsService() {
    try {
      gitHub = GitHubBuilder.fromPropertyFile().build();
    } catch (IOException e) {
      log.warn("Can't create github API client... Check existence of properties file ~/.github", e);
    }
  }

  /**
   * Polls the commits made by the 'author' starting from 'startingDateTime'
   *
   * @param author author the commits
   * @param startingDateTime starting creation date of the commits
   * @return stream of commits
   */
  public Stream<Commit> poll(String author, LocalDateTime startingDateTime) {
    String startingDateTimeStr = String.format(">%s",
        startingDateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")));

    log.info("Polling commits for " + author + ". Starting date: " + startingDateTimeStr);

    Iterable<GHCommit> ghCommitsIterable = gitHub.searchCommits()
        .author(author)
        .authorDate(startingDateTimeStr)
        .list();

    return StreamSupport.stream(ghCommitsIterable.spliterator(), false)
        .map(this::convertGHCommitToCommitModel);
  }

  @SneakyThrows
  private Commit convertGHCommitToCommitModel (GHCommit ghCommit) {
    return new Commit()
        .setAuthor(ghCommit.getAuthor().getLogin())
        .setDateTime(fromDateToLocalDateTime(ghCommit.getCommitDate()))
        .setSha(ghCommit.getSHA1())
        .setLanguage(ghCommit.getOwner().getLanguage())
        .setMessage(ghCommit.getCommitShortInfo().getMessage())
        .setRepository(ghCommit.getOwner().getName());
  }

  private LocalDateTime fromDateToLocalDateTime(Date date) {
    return LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
  }

}
