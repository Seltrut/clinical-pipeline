package com.clinical.consumerservice;

import com.clinical.consumerservice.dto.ClinicalObservationEvent;
import com.clinical.consumerservice.repository.ClinicalObservationRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Executors;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@Testcontainers
@DirtiesContext
class ClinicalEventConsumerIntegrationTest {

    @Container
    static final ConfluentKafkaContainer KAFKA = new ConfluentKafkaContainer(
        DockerImageName.parse("confluentinc/cp-kafka:7.6.0")
    );

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>(
        DockerImageName.parse("postgres:16-alpine")
    );

    @Container
    @SuppressWarnings("resource") // Testcontainers closes static containers after the test class.
    static final GenericContainer<?> REDIS = new GenericContainer<>(
        DockerImageName.parse("redis:7-alpine")
    ).withExposedPorts(6379);

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
    }

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private ClinicalObservationRepository repository;

    @Autowired
    private StringRedisTemplate redisTemplate;

    @BeforeEach
    void cleanState() {
        repository.deleteAll();
    }

    @Test
    @DisplayName("New observation is consumed from Kafka, persisted to PostgreSQL, and cached in Redis")
    void shouldConsumeAndPersistObservation() {
        var event = new ClinicalObservationEvent(
            "evt-consumer-test-1",
            "pat-99001",
            "8480-6",
            "Systolic Blood Pressure",
            120.0,
            "mmHg",
            Instant.now()
        );

        // Publish event to Kafka
        kafkaTemplate.send("clinical.events.raw", event.patientId(), event);

        // Wait asynchronously for consumer to process the event
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            var entity = repository.findAll().stream()
                .filter(e -> "evt-consumer-test-1".equals(e.getEventId()))
                .findFirst();

            assertThat(entity).isPresent();
            assertThat(entity.get().getPatientId()).isEqualTo("pat-99001");
            assertThat(entity.get().getValue()).isEqualTo(120.0);
            assertThat(entity.get().getProcessedAt()).isNotNull();

            // Verify Redis key exists
            Boolean hasKey = redisTemplate.hasKey("processed_event:evt-consumer-test-1");
            assertThat(hasKey).isTrue();
        });
    }

    @Test
    @DisplayName("Duplicate observation is intercepted by Redis and not duplicated in PostgreSQL")
    void shouldSkipDuplicateObservation() {
        var event = new ClinicalObservationEvent(
            "evt-duplicate-test",
            "pat-99002",
            "8480-6",
            "Systolic Blood Pressure",
            124.0,
            "mmHg",
            Instant.now()
        );

        // Pre-populate Redis to simulate that this event was already processed
        redisTemplate.opsForValue().set("processed_event:evt-duplicate-test", "PROCESSED", Duration.ofHours(24));

        // Publish to Kafka
        kafkaTemplate.send("clinical.events.raw", event.patientId(), event);

        // Wait 3 seconds to let Kafka process, then assert no database write occurred
        await().during(3, TimeUnit.SECONDS).atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            boolean exists = repository.existsByEventId("evt-duplicate-test");
            assertThat(exists).isFalse();
            assertThat(repository.count()).isZero();
        });
    }

    @Test
    @DisplayName("Concurrent duplicate events are atomically filtered: exactly 1 database write, 9 rejected")
    void shouldHandleConcurrentDuplicateEventsAtomically() throws Exception {
        var event = new ClinicalObservationEvent(
            "evt-race-condition-99",
            "pat-99003",
            "8480-6",
            "Systolic Blood Pressure",
            122.0,
            "mmHg",
            Instant.now()
        );

        int threadCount = 10;
        var executor = Executors.newFixedThreadPool(threadCount);
        var latch = new CountDownLatch(threadCount);

        // Fire 10 identical messages into Kafka simultaneously across 10 threads
        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    kafkaTemplate.send("clinical.events.raw", event.patientId(), event);
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(5, TimeUnit.SECONDS);
        executor.shutdown();

        // Assert that out of 10 concurrent deliveries, EXACTLY 1 row exists in PostgreSQL
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            long count = repository.findAll().stream()
                .filter(e -> "evt-race-condition-99".equals(e.getEventId()))
                .count();

            assertThat(count).isEqualTo(1);
        });
    }

    @Test
    @DisplayName("Poison pill record with impossible clinical value is retried and routed to DLQ")
    void shouldRoutePoisonPillToDeadLetterQueue() {
        var poisonEvent = new ClinicalObservationEvent(
            "evt-poison-pill-001",
            "pat-bad-data",
            "8480-6",
            "Systolic Blood Pressure",
            -50.0, // Impossible negative blood pressure
            "mmHg",
            Instant.now()
        );

        // Set up a consumer specifically listening to the DLQ topic
        Map<String, Object> dlqConsumerProps = org.springframework.kafka.test.utils.KafkaTestUtils.consumerProps(
            KAFKA.getBootstrapServers(), "dlq-verify-group", "true"
        );
        dlqConsumerProps.put(org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);
        dlqConsumerProps.put(org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);
        dlqConsumerProps.put(org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        var dlqConsumer = new org.springframework.kafka.core.DefaultKafkaConsumerFactory<String, String>(dlqConsumerProps).createConsumer();
        dlqConsumer.subscribe(java.util.Collections.singletonList("clinical.events.dlq"));

        // Send the bad record
        kafkaTemplate.send("clinical.events.raw", poisonEvent.patientId(), poisonEvent);

        // Assert that the record landed in the DLQ within 10 seconds
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            var dlqRecord = org.springframework.kafka.test.utils.KafkaTestUtils.getSingleRecord(
                dlqConsumer, "clinical.events.dlq", Duration.ofSeconds(5)
            );

            assertThat(dlqRecord).isNotNull();
            assertThat(dlqRecord.key()).isEqualTo("pat-bad-data");
            assertThat(dlqRecord.value()).contains("evt-poison-pill-001");

            // Verify it was never written to PostgreSQL
            boolean existsInDb = repository.existsByEventId("evt-poison-pill-001");
            assertThat(existsInDb).isFalse();
        });

        dlqConsumer.close();
    }

    @Test
    @DisplayName("Consumer recovers after a poison pill and processes subsequent valid events on the same partition")
    void shouldRecoverAndProcessValidEventAfterPoisonPill() {
        String patientId = "pat-recovery-test";

        var poisonEvent = new ClinicalObservationEvent(
            "evt-poison-999",
            patientId,
            "8480-6",
            "Systolic Blood Pressure",
            999.0, // Out of bounds (> 350.0)
            "mmHg",
            Instant.now()
        );

        var validEvent = new ClinicalObservationEvent(
            "evt-valid-1000",
            patientId,
            "8867-4",
            "Heart rate",
            72.0, // Valid heart rate
            "/min",
            Instant.now()
        );

        // Send poison pill followed immediately by a valid event for the same patient
        kafkaTemplate.send("clinical.events.raw", patientId, poisonEvent);
        kafkaTemplate.send("clinical.events.raw", patientId, validEvent);

        // Assert the poison pill was rejected and the valid event was persisted
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(repository.existsByEventId("evt-poison-999")).isFalse();
            assertThat(repository.existsByEventId("evt-valid-1000")).isTrue();
        });
    }
}