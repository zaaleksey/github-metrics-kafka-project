package com.gridu.kafka.course.github;

import com.gridu.kafka.course.github.model.Account;
import com.gridu.kafka.course.github.model.Commit;
import com.gridu.kafka.course.github.service.AccountsConsumer;
import com.gridu.kafka.course.github.service.CommitsProducer;
import com.gridu.kafka.course.github.service.GitHubCommitsService;
import com.gridu.kafka.course.github.service.IntervalDeserializer;
import java.time.Duration;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AccountsAnalyzerApp {

  private static final String INPUT_TOPIC = "github-accounts";
  private static final String OUTPUT_TOPIC = "github-commits";
  private static final String GROUP_ID = "github-accounts-analyzer";
  private static boolean shutdownFlag = false;
  private AccountsConsumer accountsConsumer;
  private IntervalDeserializer intervalDeserializer;
  private GitHubCommitsService commitsService;
  private CommitsProducer commitsProducer;

  public AccountsAnalyzerApp(String bootstrapServer) {
    accountsConsumer = new AccountsConsumer(bootstrapServer, GROUP_ID);
    accountsConsumer.subscribe(INPUT_TOPIC);
    intervalDeserializer = new IntervalDeserializer();
    commitsService = new GitHubCommitsService();
    commitsProducer = new CommitsProducer(bootstrapServer, OUTPUT_TOPIC);

    addShutdownHook();
  }

  public void run() {
    Stream<Account> accountStream = accountsConsumer.poll(Duration.ofMillis(1000));
    Stream<Commit> commitStream = accountStream.flatMap(
        account -> commitsService.poll(account.getAccount(),
            intervalDeserializer.startingDateTime(account.getInterval())));
    commitStream.forEach(commitsProducer::send);
  }

  public void close() {
    log.info("Shutting down...");
    accountsConsumer.close();
    commitsProducer.close();
  }

  private void addShutdownHook() {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      shutdownFlag = true;
      try {
        Thread.sleep(3000);
      } catch (InterruptedException e) {
        log.warn("Error occurred when shutting down", e);
      }
    }));
  }

  public static void main(String[] args) {
    String bootstrapServer = "localhost:9092,localhost:9093,localhost:9094";
    AccountsAnalyzerApp app = new AccountsAnalyzerApp(bootstrapServer);

    try {
      while (!shutdownFlag) {
        app.run();
      }
    } finally {
      app.close();
    }
  }

}
