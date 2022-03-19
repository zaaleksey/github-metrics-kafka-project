package com.gridu.kafka.course.github.api;

import com.gridu.kafka.course.github.model.Commit;
import lombok.SneakyThrows;
import org.kohsuke.github.GHCommit;
import org.kohsuke.github.GitHub;
import org.kohsuke.github.GitHubBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class GitHubCommitsService {

    private static final Logger logger = LoggerFactory.getLogger(GitHubCommitsService.class);

    private GitHub gitHub;

    public GitHubCommitsService() {
        try {
            gitHub = GitHubBuilder.fromPropertyFile().build();
        } catch (IOException e) {
            logger.warn("Can't create github API client... Check existence of properties file ~/.github", e);
        }
    }

    public Stream<Commit> poll(String author, LocalDateTime startingDateTime) {
        String startingDateTimeStr = String.format(">%s",
                startingDateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")));

        logger.info("Polling commits for " + author + ". Starting date: " + startingDateTimeStr);

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
                .setDateTime(ghCommit.getCommitDate())
                .setSha(ghCommit.getSHA1())
                .setLanguage(ghCommit.getOwner().getLanguage())
                .setMessage(ghCommit.getCommitShortInfo().getMessage())
                .setRepository(ghCommit.getOwner().getName());
    }

}
