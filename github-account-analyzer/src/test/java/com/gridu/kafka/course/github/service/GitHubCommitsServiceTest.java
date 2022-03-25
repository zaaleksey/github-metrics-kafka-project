package com.gridu.kafka.course.github.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import com.gridu.kafka.course.github.model.Commit;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Spliterator;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.kohsuke.github.GHCommit;
import org.kohsuke.github.GitHub;
import org.kohsuke.github.PagedSearchIterable;
import org.mockito.Answers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class GitHubCommitsServiceTest {

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private GHCommit ghCommitMock;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private GitHub gitHubMock;

  @InjectMocks
  private GitHubCommitsService commitsServiceUnderTest;

  @Test
  void poll() {
    LocalDateTime dateTime = LocalDateTime.now().withNano(0);
    Commit commit = new Commit()
        .setAuthor("author")
        .setDateTime(dateTime)
        .setSha("sha")
        .setLanguage("language")
        .setMessage("message")
        .setRepository("repository");

    GHCommit ghCommit = getGHCommitMock(commit);
    Spliterator<GHCommit> ghCommitSpliterator = Collections.singletonList(ghCommit).spliterator();

    PagedSearchIterable<GHCommit> ghCommitIterable = mock(PagedSearchIterable.class);
    when(ghCommitIterable.spliterator()).thenReturn(ghCommitSpliterator);

    when(gitHubMock.searchCommits().author(any()).authorDate(any()).list())
        .thenReturn(ghCommitIterable);

    List<Commit> expected = Collections.singletonList(commit);
    List<Commit> actual = commitsServiceUnderTest.poll(commit.getAuthor(),
        commit.getLocalDateTime()).collect(Collectors.toList());
    assertEquals(expected, actual);
  }

  @SneakyThrows
  private GHCommit getGHCommitMock(Commit commit) {
    Date commitDate = Date.from(commit.getLocalDateTime().atZone(ZoneId.systemDefault())
        .toInstant());

    when(ghCommitMock.getAuthor().getLogin())
        .thenReturn(commit.getAuthor());
    when(ghCommitMock.getCommitDate())
        .thenReturn(commitDate);
    when(ghCommitMock.getSHA1())
        .thenReturn(commit.getSha());
    when(ghCommitMock.getOwner().getLanguage())
        .thenReturn(commit.getLanguage());
    when(ghCommitMock.getOwner().getName())
        .thenReturn(commit.getRepository());
    when(ghCommitMock.getCommitShortInfo().getMessage())
        .thenReturn(commit.getMessage());

    return ghCommitMock;
  }

}