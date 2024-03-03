package com.kafka.springbootkafka.service.microservice1;


import com.kafka.springbootkafka.model.User;
import com.kafka.springbootkafka.service.JsonKafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@Service

public class Producer {


    private static final Logger LOGGER = LoggerFactory.getLogger(Producer.class);

    private KafkaTemplate<String, User> kafkaTemplate;

    public Producer(KafkaTemplate<String, User> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

//    @PostMapping("sendmessage")
//    public void sendMessage(@RequestBody User data, @RequestParam String topic) {
//
//
//            LOGGER.info(String.format("Message sent to Topic Two -> %s", data.toString()));
//
//            Message<User> message = MessageBuilder
//                    .withPayload(data)
//                    .setHeader(KafkaHeaders.TOPIC, topic)
//                    .build();
//
//            kafkaTemplate.send(message);
//
//
//    }


//  @PostMapping("sendmessage")

    public void sendMessage(User data){

        LOGGER.info(String.format("Message sent to Topic Two -> %s" ,data.toString()));

        Message<User> message = MessageBuilder
                .withPayload(data)
                .setHeader(KafkaHeaders.TOPIC, "kafkaTopic")
                .build();

        kafkaTemplate.send(message);
      //  kafkaTemplate.send("kafkaTopic",data);
    }
}
