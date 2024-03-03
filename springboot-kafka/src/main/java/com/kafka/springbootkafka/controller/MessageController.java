package com.kafka.springbootkafka.controller;

import com.kafka.springbootkafka.service.ConsumeSomeData;
import com.kafka.springbootkafka.service.KafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping("/api/test")
public class MessageController {

    private KafkaProducer kafkaProducer;
    private ConsumeSomeData consumeSomeData;

    public MessageController(KafkaProducer kafkaProducer, ConsumeSomeData consumeSomeData) {
        this.kafkaProducer = kafkaProducer;
        this.consumeSomeData = consumeSomeData;
    }

    //http:localhost:8090/api/v1/kafka/publish?message=hello World
    @PostMapping("/sendMessage")
    public ResponseEntity<String> send(@RequestParam("topic") String topic,@RequestParam("message") String message){
        kafkaProducer.sendMessage(topic,message);
        return ResponseEntity.ok("Message sent to the topic");
    }
//    @GetMapping("/consumeMessage")
//    public ResponseEntity<List<String>> consume(@RequestParam("topic") String topic) {
//        List<String> consumedMessages = consumeSomeData.getConsumedMessages();
//        return ResponseEntity.ok(consumedMessages);
//    }

    @GetMapping("/consume/{topic}")
    public ResponseEntity<List<String>> consumeMessages(@PathVariable String topic) {
        consumeSomeData.subscribeToTopic(topic);

        List<String> messages = new ArrayList<>();
        ConsumerRecords<String, String> records = consumeSomeData.consumeAllMessages();

        for (ConsumerRecord<String, String> record : records) {
            messages.add(record.value());
        }

        return ResponseEntity.ok(messages);
    }

}




