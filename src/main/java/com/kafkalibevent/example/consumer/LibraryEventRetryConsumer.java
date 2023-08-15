
package com.kafkalibevent.example.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kafkalibevent.example.service.LibraryEventService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class LibraryEventRetryConsumer {

    @Autowired
    private LibraryEventService service;

    @KafkaListener(topics = "${topics.retry.name}", groupId = "${topics.retry.group-id}",
            autoStartup = "${topics.retry.auto-startup:true}")
    void onMessage(ConsumerRecord<Integer, String> record) throws JsonProcessingException {
        log.info("ConsumerRecord in Retry Listener: {}", record);

        for (Header header : record.headers()) {
            log.info("Header key: {}, value = {}", header.key(), new String(header.value()));
        }

        service.processLibraryEvent(record);

    }
}

