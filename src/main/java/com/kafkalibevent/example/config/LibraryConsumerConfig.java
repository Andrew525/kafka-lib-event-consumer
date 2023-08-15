package com.kafkalibevent.example.config;

import com.kafkalibevent.example.service.FailureService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.ContainerCustomizer;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.*;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

import java.util.function.BiFunction;

@EnableKafka
@Configuration
@Slf4j
public class LibraryConsumerConfig {

    @Autowired
    KafkaTemplate kafkaTemplate;

    @Autowired
    FailureService failureService;

    @Value("${topics.retry.name}")
    String retryTopic;

    @Value("${topics.dlt.name}")
    String deadLetterTopic;


    public static final String RETRY_STATUS = "RETRY";
    public static final String DEAD_STATUS = "DEAD";

    @Bean
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            KafkaProperties properties,
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ObjectProvider<ConsumerFactory<Object, Object>> kafkaConsumerFactory,
            ObjectProvider<ContainerCustomizer<Object, Object, ConcurrentMessageListenerContainer<Object, Object>>> kafkaContainerCustomizer) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory
                .getIfAvailable(() -> new DefaultKafkaConsumerFactory<>(properties.buildConsumerProperties())));
        kafkaContainerCustomizer.ifAvailable(factory::setContainerCustomizer);

        factory.setConcurrency(3);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH);
        factory.setCommonErrorHandler(errorHandler());

        return factory;
    }

    private CommonErrorHandler errorHandler() {
        var expBackOff = new ExponentialBackOffWithMaxRetries(2);
        expBackOff.setInitialInterval(1_000L);
        expBackOff.setMultiplier(2.0);
        expBackOff.setMaxInterval(2_000L);

        var errorHandler = new DefaultErrorHandler(
//                recoverer,
                publishingRecoverer(),
//                new FixedBackOff(1_000L, 2);
                expBackOff
        );

        errorHandler.addNotRetryableExceptions(IllegalArgumentException.class);

        errorHandler.setRetryListeners((record, ex, deliveryAttempt) ->
                log.info("Filed Record in Retry Listener, deliveryAttempt {}, Exception {}", deliveryAttempt, ex.getMessage()));
        return errorHandler;
    }

    ConsumerRecordRecoverer recoverer = (r, ex) -> {
        log.info("Exception in Consumer recoverer : {}", ex.getMessage());
        var record = (ConsumerRecord<Integer, String>) r;
        if (ex.getCause() instanceof RecoverableDataAccessException) {
            log.info("Recovery record");
            failureService.saveFailedRecord(record, ex, RETRY_STATUS);
        } else {
            log.info("Non-Recovery record");
            failureService.saveFailedRecord(record, ex, DEAD_STATUS);
        }
    };

    public DeadLetterPublishingRecoverer publishingRecoverer() {

        BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> recoverLogic = (r, ex) -> {
            log.info("DeadLetterPublishingRecoverer exception : {}", ex.getMessage());
            if (ex.getCause() instanceof RecoverableDataAccessException) {
                return new TopicPartition(retryTopic, r.partition());
            } else {
                return new TopicPartition(deadLetterTopic, r.partition());
            }
        };

        return new DeadLetterPublishingRecoverer(kafkaTemplate, recoverLogic);
    }

}
