package com.gridu.kafka.course.github.demo;

import com.gridu.kafka.course.github.model.Account;
import com.gridu.kafka.course.github.service.AccountsConsumer;

import java.time.Duration;
import java.util.stream.Stream;

public class AccountConsumerDemo {

    public static void main(String[] args) {
        String topic = "github-accounts";
        AccountsConsumer accountsConsumer = new AccountsConsumer(
                "localhost:9092,localhost:9093,localhost:9094", "github-accounts-analyzer");
        accountsConsumer.subscribe(topic);

        try {
            while (true) {
                Stream<Account> poll = accountsConsumer.poll(Duration.ofMillis(100));
                poll.forEach(System.out::println);
            }
        } finally {
            accountsConsumer.close();
        }
    }

}
