package com.kafka.springbootkafka.service.microservice1;

import com.kafka.springbootkafka.model.User;
//import com.kafka.springbootkafka.service.JsonKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@EnableKafka
public class Consummer {

    private final Producer kafka;


    private static final Logger LOGGER = LoggerFactory.getLogger(Consummer.class);


    public Consummer(Producer kafka) {
        this.kafka = kafka;
    }


    @KafkaListener(topics = "test" , groupId = "Group_id")
    public void consume(User user){
        //kafka.sendMessage();
        LOGGER.info(String.format("Json message received from Topic One -> %s",user.toString()));
       kafka.sendMessage(user);

    }


}
