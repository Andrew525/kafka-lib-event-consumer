package com.kafkalibevent.example.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;

@Slf4j
//@Component
public class LibraryEventConsumerManualOffset implements AcknowledgingMessageListener<Integer, String> {

    @Override
    @KafkaListener(topics = "${topics.main.name}")
    public void onMessage(ConsumerRecord<Integer, String> record, Acknowledgment acknowledgment) {
        log.info("ConsumerRecord : {}", record);
        acknowledgment.acknowledge();
    }
}

