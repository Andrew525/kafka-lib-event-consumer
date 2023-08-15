package com.kafkalibevent.example.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkalibevent.example.entity.LibraryEvent;
import com.kafkalibevent.example.jpa.LibraryEventRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.retry.RetryException;
import org.springframework.stereotype.Service;

import java.util.Objects;
import java.util.Optional;

@Service
@Slf4j
public class LibraryEventService {


    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    LibraryEventRepository repository;

    public void processLibraryEvent(ConsumerRecord<Integer, String> record) throws JsonProcessingException {
        var libraryEvent = objectMapper.readValue(record.value(), LibraryEvent.class);
        log.info("LibraryEvent : {}", libraryEvent);

        if(libraryEvent.getLibraryEventId()!=null && ( libraryEvent.getLibraryEventId()==999 )){
            throw new RecoverableDataAccessException("Temp network issue");
        }

        switch (libraryEvent.getLibraryEventType()) {
            case NEW -> save(libraryEvent);
            case UPDATE -> {
                //update:
                validate(libraryEvent);
                save(libraryEvent);
            }
            default -> log.info("Invalid library event type");
        }
    }

    private void validate(LibraryEvent libraryEvent) {
        if (libraryEvent.getLibraryEventId() == null) {
            throw new IllegalArgumentException("Library event id  is missing");
        }

        var optLibraryEvent = repository.findById(libraryEvent.getLibraryEventId());
        if (!optLibraryEvent.isPresent()) {
            throw new IllegalArgumentException("Not valid library event");
        }

        log.info("Validation is done fro libEvent: {}", libraryEvent);
    }

    private void save(LibraryEvent libraryEvent) {
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        repository.save(libraryEvent);
        log.info("Successfully persisted library event: {}", libraryEvent);
    }
}
