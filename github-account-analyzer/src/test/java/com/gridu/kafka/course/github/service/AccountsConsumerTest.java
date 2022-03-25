package com.gridu.kafka.course.github.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gridu.kafka.course.github.model.Account;
import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterators;
import java.util.stream.Stream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AccountsConsumerTest {

  private final ObjectMapper mapper = new ObjectMapper();

  @Mock
  private KafkaConsumer<String, String> kafkaConsumer;

  private AccountsConsumer accountsConsumerUnderTest;

  @BeforeEach
  void setUpAccountsConsumer() {
    accountsConsumerUnderTest = new AccountsConsumer("localhost:9090", "groupId");
    accountsConsumerUnderTest.consumer = kafkaConsumer;
  }

  @Test
  void subscribe() {
    accountsConsumerUnderTest.subscribe("topic");
    verify(kafkaConsumer, times(1)).subscribe(Collections.singletonList("topic"));
  }

  @Test
  void close() {
    accountsConsumerUnderTest.close();
    verify(kafkaConsumer, times(1)).close();
  }

  @Test
  void poll() throws JsonProcessingException {
    Account account = new Account();
    account.setAccount("author");
    account.setInterval("1d");

    String accountJson = mapper.writeValueAsString(account);
    ConsumerRecords<String, String> consumerRecords = getConsumerRecordsMock(accountJson);

    when(kafkaConsumer.poll(any())).thenReturn(consumerRecords);

    Stream<Account> accountStream = accountsConsumerUnderTest.poll(Duration.ofMillis(1000));
    Iterator<Account> accountIterator = accountStream.iterator();
    assertEquals(account, accountIterator.next());
    assertFalse(accountIterator.hasNext());
  }

  @Test
  void pollIncorrectAccount() {
    ConsumerRecords<String, String> consumerRecords = getConsumerRecordsMock("");

    when(kafkaConsumer.poll(any())).thenReturn(consumerRecords);

    assertEquals(accountsConsumerUnderTest.poll(Duration.ofMillis(1000)).count(), 0);

  }

  private ConsumerRecords<String, String> getConsumerRecordsMock(String accountJson) {
    ConsumerRecords<String, String> recordsMock = mock(ConsumerRecords.class);
    ConsumerRecord<String, String> record =
        new ConsumerRecord<>("topic", 1, 0, null, accountJson);
    List<ConsumerRecord<String, String>> records = Collections.singletonList(record);

    when(recordsMock.iterator()).thenReturn(records.iterator());
    lenient().when(recordsMock.spliterator())
        .thenReturn(Spliterators.spliteratorUnknownSize(records.iterator(), 0));

    return recordsMock;
  }

}