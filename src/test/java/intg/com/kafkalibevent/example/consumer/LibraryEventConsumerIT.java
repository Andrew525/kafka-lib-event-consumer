package com.kafkalibevent.example.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkalibevent.example.entity.Book;
import com.kafkalibevent.example.entity.LibraryEvent;
import com.kafkalibevent.example.entity.LibraryEventType;
import com.kafkalibevent.example.jpa.FailureRecordRepository;
import com.kafkalibevent.example.jpa.LibraryEventRepository;
import com.kafkalibevent.example.service.LibraryEventService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;

@SpringBootTest
@EmbeddedKafka(topics = {"${topics.main.name}", "${topics.dlt.name}", "${topics.retry.name}"}, partitions = 3)
@TestPropertySource(properties = {
        "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "topics.retry.auto-startup=false"})
class LibraryEventConsumerIT {

    @Value("${topics.retry.name}")
    String retryTopic;

    @Value("${topics.main.group-id}")
    String mainTopicGroup;

    @Value("${topics.dlt.name}")
    String dltTopic;

    @Autowired
    EmbeddedKafkaBroker kafkaBroker;

    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    KafkaListenerEndpointRegistry endpointRegistry;

    @Autowired
    LibraryEventRepository repository;

    @Autowired
    FailureRecordRepository failureRecordRepository;

    @Autowired
    ObjectMapper objectMapper;

    @SpyBean
    LibraryEventConsumer consumerSpy;

    @SpyBean
    LibraryEventService serviceSpy;


    @BeforeEach
    void setUp() {

        endpointRegistry.getAllListenerContainers()
                .stream()
                .filter(container -> Objects.equals(container.getGroupId(), mainTopicGroup))
                .forEach(c -> ContainerTestUtils.waitForAssignment(c, kafkaBroker.getPartitionsPerTopic()));

//        for (MessageListenerContainer container : endpointRegistry.getAllListenerContainers()) {
//            ContainerTestUtils.waitForAssignment(container, kafkaBroker.getPartitionsPerTopic());
//        }
    }

    @AfterEach
    void tearDown() {
        repository.deleteAll();
    }

    @Test
    void publishNewLibraryEvent() throws Exception {
        //given
        var json = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":456,\"bookAuthor\":\"Dilip\",\"bookName\":\"Kafka Using Spring Boot\"}}";

        //when
        kafkaTemplate.sendDefault(json).get();
        Thread.sleep(3000);

        //then
        verify(consumerSpy).onMessage(any(ConsumerRecord.class));
        verify(serviceSpy).processLibraryEvent(any(ConsumerRecord.class));

        var all = (List<LibraryEvent>) repository.findAll();
        assertEquals(1, all.size());
        var first = all.get(0);
        assertNotNull(first.getLibraryEventId());
        assertEquals(456, first.getBook().getBookId());

    }


    @Test
    void publishUpdateLibraryEvent() throws Exception {
        //given
        var json = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":456,\"bookAuthor\":\"Dilip\",\"bookName\":\"Kafka Using Spring Boot\"}}";
        var libraryEvent = objectMapper.readValue(json, LibraryEvent.class);
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        repository.save(libraryEvent);

        var newBook = Book.builder()
                .bookId(456)
                .bookAuthor("Test")
                .bookName("New Book Name")
                .build();
//        newBook.setLibraryEvent(libraryEvent);
        libraryEvent.setBook(newBook);
        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        var updatedEventJson = objectMapper.writeValueAsString(libraryEvent);

        //when
        kafkaTemplate.sendDefault(libraryEvent.getLibraryEventId(), updatedEventJson).get();
        Thread.sleep(3000); // 3 seconds

        //then
        verify(consumerSpy).onMessage(isA(ConsumerRecord.class));
        verify(serviceSpy).processLibraryEvent(isA(ConsumerRecord.class));

        var all = (List<LibraryEvent>) repository.findAll();
        assertEquals(1, all.size());
        var persistedLibEvent = repository.findById(libraryEvent.getLibraryEventId()).get();
        assertEquals(newBook.getBookName(), persistedLibEvent.getBook().getBookName());
        assertEquals(newBook.getBookAuthor(), persistedLibEvent.getBook().getBookAuthor());

    }

    @Test
    void publishModifyLibraryEvent_Not_A_Valid_LibraryEventId() throws JsonProcessingException, InterruptedException, ExecutionException {
        //given
        Integer libraryEventId = 123;
        String json = "{\"libraryEventId\":" + libraryEventId + ",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
        System.out.println(json);
        kafkaTemplate.sendDefault(libraryEventId, json).get();
        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(5, TimeUnit.SECONDS);


        verify(consumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(serviceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

        Optional<LibraryEvent> libraryEventOptional = repository.findById(libraryEventId);
        assertFalse(libraryEventOptional.isPresent());

        Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("group2", "true", kafkaBroker));
        var consumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(), new StringDeserializer()).createConsumer();
        kafkaBroker.consumeFromAnEmbeddedTopic(consumer, dltTopic);

        ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(consumer, dltTopic);

        System.out.println("consumer Record in deadletter topic : " + consumerRecord.value());

        assertEquals(json, consumerRecord.value());
        consumerRecord.headers()
                .forEach(header -> {
                    System.out.println("Header Key : " + header.key() + ", Header Value : " + new String(header.value()));
                });
    }

    @Test
    void publishUpdateLibraryEvent_nullEvent() throws Exception {
        //given
        Integer libraryEventId = null;
        String json = "{\"libraryEventId\":" + libraryEventId + ",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
        kafkaTemplate.sendDefault(libraryEventId, json).get();
        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);


        verify(consumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(serviceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

        Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("group3", "true", kafkaBroker));
        var consumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(), new StringDeserializer()).createConsumer();
        kafkaBroker.consumeFromAnEmbeddedTopic(consumer, dltTopic);

        ConsumerRecords<Integer, String> consumerRecords = KafkaTestUtils.getRecords(consumer);

        var deadletterList = new ArrayList<ConsumerRecord<Integer, String>>();
        consumerRecords.forEach((record) -> {
            if (record.topic().equals(dltTopic)) {
                deadletterList.add(record);
            }
        });

        var actualCount = deadletterList
                .stream()
                .filter(record -> record.value().equals(json))
                .count();

        assertEquals(1, actualCount);

    }

    @Test
    void publishUpdateLibraryEvent_999Event() throws Exception {
        //given
        var json = "{\"libraryEventId\":999,\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookAuthor\":\"Dilip\",\"bookName\":\"Kafka Using Spring Boot\"}}";

        //when
        kafkaTemplate.sendDefault(json).get();
        Thread.sleep(5000);

        //then
        verify(consumerSpy, times(3)).onMessage(isA(ConsumerRecord.class));
        verify(serviceSpy, times(3)).processLibraryEvent(isA(ConsumerRecord.class));
    }

    @Test
    void publishUpdateLibraryEvent_DeadEvent() throws Exception {
        //given
        Integer libraryEventId = 999;
        String json = "{\"libraryEventId\":" + libraryEventId + ",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";

        // consumer for testing purposes
        Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("group1", "true", kafkaBroker));
        var consumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(), new StringDeserializer()).createConsumer();
        kafkaBroker.consumeFromAnEmbeddedTopic(consumer, retryTopic);


        //when
        kafkaTemplate.sendDefault(libraryEventId, json).get();
        Thread.sleep(5000);

        //with Retry listener
        verify(consumerSpy, atLeast(3)).onMessage(isA(ConsumerRecord.class));
        verify(serviceSpy, atLeast(3)).processLibraryEvent(isA(ConsumerRecord.class));


        ConsumerRecords<Integer, String> allRecords = KafkaTestUtils.getRecords(consumer);
        assertEquals(1, allRecords.count());

        for (var record : allRecords) {
            System.out.println("Records from retry topic : " + record.value());
            assertEquals(json, record.value());

            record.headers()
                    .forEach(header -> {
                        System.out.println("Header Key : " + header.key() + ", Header Value : " + new String(header.value()));
                    });
        }

    }


    @Test
    @Disabled
    void publishModifyLibraryEvent_999_LibraryEventId_failureRecord() throws JsonProcessingException, InterruptedException, ExecutionException {
        //given
        Integer libraryEventId = 999;
        String json = "{\"libraryEventId\":" + libraryEventId + ",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
        kafkaTemplate.sendDefault(libraryEventId, json).get();
        //when
        Thread.sleep(5000);


        verify(consumerSpy, times(3)).onMessage(isA(ConsumerRecord.class));
        verify(serviceSpy, times(3)).processLibraryEvent(isA(ConsumerRecord.class));


        var failureCount = failureRecordRepository.count();
        assertEquals(1, failureCount);
        failureRecordRepository.findAll().forEach(failureRecord -> {
            System.out.println("failureRecord : " + failureRecord);
        });

    }
}