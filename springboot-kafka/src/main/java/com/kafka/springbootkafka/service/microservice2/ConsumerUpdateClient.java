package com.kafka.springbootkafka.service.microservice2;

import com.kafka.springbootkafka.model.User;
import com.kafka.springbootkafka.service.microservice1.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;


@Service
@EnableKafka
public class ConsumerUpdateClient {


    private final ProducerUpdateClient producerUpdateClient;


    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerUpdateClient.class);


    public ConsumerUpdateClient(ProducerUpdateClient producerUpdateClient) {
        this.producerUpdateClient = producerUpdateClient;
    }

    @KafkaListener(topics = "kafkaTopic" , groupId = "Group_id")

    public void consumeMessage(User user){

        LOGGER.info(String.format("Json message received from Topic Two -> %s",user.toString()));
        producerUpdateClient.sendMessageToTopic3(user);

    }


}

