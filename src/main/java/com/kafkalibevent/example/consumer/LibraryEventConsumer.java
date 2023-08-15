package com.kafkalibevent.example.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kafkalibevent.example.service.LibraryEventService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class LibraryEventConsumer {

    @Autowired
    private LibraryEventService service;

    @KafkaListener(topics = "${topics.main.name}", groupId = "${topics.main.group-id}")
    void onMessage(ConsumerRecord<Integer, String> record) throws JsonProcessingException {
        log.info("ConsumerRecord : {}", record);

        service.processLibraryEvent(record);

    }
}

