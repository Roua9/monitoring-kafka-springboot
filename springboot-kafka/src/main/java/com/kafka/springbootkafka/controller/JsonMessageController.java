package com.kafka.springbootkafka.controller;


import com.kafka.springbootkafka.model.User;
//import com.kafka.springbootkafka.service.AddClientService;
import com.kafka.springbootkafka.service.JsonKafkaProducer;
import com.kafka.springbootkafka.service.microservice1.Consummer;
import com.kafka.springbootkafka.service.microservice1.Producer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/kafka")
public class JsonMessageController {


    private JsonKafkaProducer kafkaProducer;

    private Consummer consummer;
    private Producer producer;



    public JsonMessageController(JsonKafkaProducer kafkaProducer, Consummer consummer, Producer producer) {
        this.kafkaProducer = kafkaProducer;
        this.consummer = consummer;
        this.producer = producer;

    }

    @PostMapping("/publish")
    public ResponseEntity<String> publish(@RequestBody User user){
        kafkaProducer.sendMessage(user);
        return ResponseEntity.ok("Json message sent to kafka topic");

    }




}
