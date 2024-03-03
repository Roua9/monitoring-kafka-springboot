package com.kafka.springbootkafka.service.microservice2;


import com.kafka.springbootkafka.model.User;
import com.kafka.springbootkafka.service.microservice1.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

@Service
public class ProducerUpdateClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerUpdateClient.class);

    private KafkaTemplate<String, User> kafkaTemplate;

    public ProducerUpdateClient(KafkaTemplate<String, User> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }


    public void sendMessageToTopic3(User data){

        LOGGER.info(String.format("Message sent to Topic Three -> %s" ,data.toString()));

        Message<User> message = MessageBuilder
                .withPayload(data)
                .setHeader(KafkaHeaders.TOPIC, "endPoint")
                .build();

        kafkaTemplate.send(message);
    }
}
