package com.gridu.kafka.course.github.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gridu.kafka.course.github.model.Account;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Wrapper around KafkaConsumer.
 * Consumes GitHub accounts from github-accounts topic.
 */
public class AccountsConsumer {

    private static final Logger logger = LoggerFactory.getLogger(AccountsConsumer.class);

    private final ObjectMapper mapper;

    private KafkaConsumer<String, String> consumer;

    /**
     * Constructs AccountsConsumer with the provided parameters.
     *
     * @param bootstrapServers kafka consumer's bootstrap server
     * @param groupId kafka consumer's group id
     */
    public AccountsConsumer(String bootstrapServers, String groupId) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        mapper = new ObjectMapper();
        consumer = new KafkaConsumer<>(properties);
    }

    public void subscribe(String topic) {
        consumer.subscribe(Collections.singleton(topic));
    }

    public void close() {
        logger.info("Accounts consumer shutting down...");
        consumer.close();
    }

    /**
     * Proxies 'poll' call to the kafka consumer and maps received records to Account class.
     * {@link Account}
     *
     * @param timeout duration of poll
     * @return stream of accounts
     */
    public Stream<Account> poll(Duration timeout) {
        Iterable<ConsumerRecord<String, String>> recordIterable = () -> consumer.poll(timeout).iterator();
        return StreamSupport.stream(recordIterable.spliterator(), false)
                .map(ConsumerRecord::value)
                .map(this::jsonStringToAccount);
    }

    /**
     * Convert json string in Account object with objectMapper.
     *
     * @param json a json string describing the account
     * @return account object
     */
    private Account jsonStringToAccount(String json) {
        try {
            Account account = mapper.readValue(json, Account.class);
            logger.info("New record: " + account);
            return account;
        } catch (Exception e) {
            logger.warn("Can't read the value - data may be malformed", e);
        }
        return new Account();
    }

}
