package com.kafkalibevent.example.service;

import com.kafkalibevent.example.entity.FailureRecord;
import com.kafkalibevent.example.jpa.FailureRecordRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

@Service
public class FailureService {

    private FailureRecordRepository failureRecordRepository;

    public FailureService(FailureRecordRepository failureRecordRepository) {
        this.failureRecordRepository = failureRecordRepository;
    }

    public void saveFailedRecord(ConsumerRecord<Integer, String> record, Exception exception, String recordStatus) {
        var failureRecord = FailureRecord.builder()
                .topic(record.topic())
                .recordKey(record.key())
                .recordValue(record.value())
                .partition(record.partition())
                .offsetValue(record.offset())
                .exception(exception.getCause().getMessage())
                .status(recordStatus)
                .build();

        failureRecordRepository.save(failureRecord);

    }
}
