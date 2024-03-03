package com.kafka.springbootkafka.service;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.*;

@Service
public class ConsumeSomeData {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumeSomeData.class);
    //private List<String> consumedMessages = new ArrayList<>();
    private final KafkaConsumer<String, String> consumer;

//    @KafkaListener(topics = "${kafka.consumer.topic}" , groupId = "groupRoua")
//    public void consume(String message){
//        LOGGER.info(String.format("Message received -> %s" ,message));
//        consumedMessages.add(message);
//    }
//    public List<String> getConsumedMessages() {
//        return consumedMessages;
//    }


    public ConsumeSomeData() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        this.consumer = new KafkaConsumer<>(properties);
    }
    public void subscribeToTopic(String topicName) {
        consumer.subscribe(Collections.singletonList(topicName));
    }

    public ConsumerRecords<String, String> consumeAllMessages() {
        consumer.seekToBeginning(consumer.assignment()); // Se déplace au début du topic
        return consumer.poll(Duration.ofMillis(100)); // Récupère tous les messages
    }

    public void close() {
        consumer.close();
    }
}

