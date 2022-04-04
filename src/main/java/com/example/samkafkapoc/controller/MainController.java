package com.example.samkafkapoc.controller;

import com.example.samkafkapoc.domain.TestSum;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

@RestController
public class MainController {
    @Autowired
    KafkaTemplate template;

    private static final String TOPIC_NAME = "test-topic";

    @PostMapping("/send")
    public String sendMessage(@RequestBody String msg) {
        template.send(TOPIC_NAME, msg);

        return "Done";
    }

    @KafkaListener(topics = TOPIC_NAME,
            groupId = "consumer-2",
            containerFactory = "simpleKafkaListenerContainerFactory")
    public void testListener(String msg) {
        System.out.println("Got message: " + msg);
    }

    @Autowired
    MongoTemplate mongoTemplate;

    @KafkaListener(topics = "TEST-SUM",
            groupId = "consumer-1",
            containerFactory = "simpleKafkaListenerContainerFactory")
    public void testSum(String msg) {
        System.out.println("Got message: " + msg);
        Query query = Query.query(Criteria.where("name").is("hello"));
        var i = mongoTemplate.find(query, TestSum.class);

        if (i.isEmpty()) {
            System.out.println("Not found item");
            var o = new TestSum();
            o.name = "hello";
            o.sum = 0;
            mongoTemplate.insert(o);
            i = mongoTemplate.find(query, TestSum.class);
        }

        i.forEach(e -> {
            System.out.println("Before image: " + e.sum);
            e.sum = e.sum + Long.valueOf(msg);
            System.out.println("After image: " + e.sum);
            Update update = Update.update("sum", e.sum);
            mongoTemplate.updateFirst(query, update, TestSum.class);
//            mongoTemplate.update(TestSum.class).replaceWith(e);
        });

    }

    @GetMapping("/sum/{num}")
    public String testSum(@PathVariable("num") int num) {
        for (int i=1; i<=num; i++) {
            Integer v = i;
            CompletableFuture.supplyAsync(() -> v.toString())
                    .thenAccept(msg -> template.send("TEST-SUM", 0, "1", v.toString()));
        }
        return "Submitted";
    }
}
