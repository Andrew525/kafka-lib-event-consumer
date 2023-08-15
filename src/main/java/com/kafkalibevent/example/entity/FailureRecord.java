package com.kafkalibevent.example.entity;


import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;


@Entity
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class FailureRecord {
    @Id
    @GeneratedValue
    private Integer bookId;
    private String topic;
    private Integer recordKey;
    private String recordValue;
    private Integer partition;
    private Long offsetValue;
    private String exception;
    private String status;

}
