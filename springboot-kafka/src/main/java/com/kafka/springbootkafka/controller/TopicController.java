package com.kafka.springbootkafka.controller;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.web.bind.annotation.*;

import java.util.Collections;
import java.util.Set;

@RestController
@RequestMapping("/api/kafka")
public class TopicController {

    @Autowired
    private KafkaAdmin kafkaAdmin;

    @Autowired
    private AdminClient adminClient;


    public TopicController(KafkaAdmin kafkaAdmin, AdminClient adminClient) {
        this.kafkaAdmin = kafkaAdmin;
        this.adminClient = adminClient;
    }


    /////////////////// add Topic //////////////////////
    @PostMapping("/addTopic")
    public ResponseEntity<String> addTopic(@RequestParam("topicName") String topicName) {
        NewTopic newTopic = TopicBuilder.name(topicName)
                .partitions(1)
                .replicas(1)
                .build();

        adminClient.createTopics(Collections.singletonList(newTopic));

        return ResponseEntity.ok("Topic creation initiated");
    }


    //////////////// delete Topic ////////////
    @DeleteMapping("/deleteTopic/{topicName}")
    public ResponseEntity<String> deleteTopic(@PathVariable String topicName) {
        DeleteTopicsOptions deleteOptions = new DeleteTopicsOptions();
        deleteOptions.timeoutMs(5000); // Timeout pour la suppression
        adminClient.deleteTopics(Collections.singleton(topicName), deleteOptions).all().whenComplete((voidValue, throwable) -> {
            if (throwable == null) {
                System.out.println("Topic " + topicName + " deleted successfully");
            } else {
                System.out.println("Error deleting topic " + topicName + ": " + throwable.getMessage());
            }
        });
        return ResponseEntity.ok("Topic deletion initiated");
    }


    //////////// get list Topic ////////////
    @GetMapping("/listTopics")
    public ResponseEntity<Set<String>> listTopics() {
        ListTopicsOptions options = new ListTopicsOptions();
        ListTopicsResult topicsResult = adminClient.listTopics(options);
        KafkaFuture<Set<String>> topicNamesFuture = topicsResult.names();
        try {
            Set<String> topicNames = topicNamesFuture.get();
            return ResponseEntity.ok(topicNames);
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(500).build();
        }
    }

}
