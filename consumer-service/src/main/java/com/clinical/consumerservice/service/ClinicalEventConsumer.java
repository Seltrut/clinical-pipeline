package com.clinical.consumerservice.service;

import com.clinical.consumerservice.dto.ClinicalObservationEvent;
import com.clinical.consumerservice.exception.InvalidClinicalDataException;
import com.clinical.consumerservice.model.ClinicalObservationEntity;
import com.clinical.consumerservice.repository.ClinicalObservationRepository;
import com.clinical.consumerservice.validation.ClinicalDataValidator;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;

@Slf4j
@Service
public class ClinicalEventConsumer {

    private final ClinicalObservationRepository repository;
    private final StringRedisTemplate redisTemplate;
    private final ClinicalDataValidator validator;

    private final Counter persistedCounter;
    private final Counter duplicateCounter;
    private final Counter validationFailureCounter;
    private final Timer processingTimer;

    private static final String IDEMPOTENCY_PREFIX = "processed_event:";
    private static final Duration TTL = Duration.ofHours(24);

    public ClinicalEventConsumer(
            ClinicalObservationRepository repository,
            StringRedisTemplate redisTemplate,
            ClinicalDataValidator validator,
            MeterRegistry meterRegistry) {
        this.repository = repository;
        this.redisTemplate = redisTemplate;
        this.validator = validator;

        this.persistedCounter = Counter.builder("clinical.events.persisted.total")
            .description("Total number of valid clinical observations saved to PostgreSQL")
            .register(meterRegistry);

        this.duplicateCounter = Counter.builder("clinical.events.duplicates.total")
            .description("Total number of duplicate clinical events intercepted by Redis")
            .register(meterRegistry);

        this.validationFailureCounter = Counter.builder("clinical.events.invalid.total")
            .description("Total number of clinical events rejected due to out-of-bounds values")
            .register(meterRegistry);

        this.processingTimer = Timer.builder("clinical.events.processing.duration")
            .description("End-to-end consumer processing time per event")
            .publishPercentileHistogram()
            .register(meterRegistry);
    }

    /*
        * Kafka listener for clinical observation events.
        * Ensures idempotent processing using Redis to track processed event IDs.
        * If an event has already been processed, it will be skipped to prevent duplicate database writes.
    */
    @KafkaListener(topics = "${app.kafka.topics.clinical-events}", groupId = "${spring.kafka.consumer.group-id}")
    public void processClinicalEvent(ClinicalObservationEvent event) {
        processingTimer.record(() -> {
            log.info("Recieved event {} for patient {}", event.eventId(), event.patientId());

            try {
                validator.validate(event);
            } catch(InvalidClinicalDataException e) {
                validationFailureCounter.increment();
                throw e; // Rethrow to let the error handler manage it (e.g., send to DLQ)
            }

            String redisKey = IDEMPOTENCY_PREFIX + event.eventId();
            Boolean isNewEvent = redisTemplate.opsForValue().setIfAbsent(redisKey, "PROCESSED", TTL);

            if (Boolean.FALSE.equals(isNewEvent)) {
                log.warn("Duplicate event detected: {}. Skipping database write.", event.eventId());
                duplicateCounter.increment();
                return;
            }

            // Persist to PostgreSQL
            ClinicalObservationEntity entity = ClinicalObservationEntity.builder()
                .eventId(event.eventId())
                .patientId(event.patientId())
                .code(event.code())
                .display(event.display())
                .value(event.value())
                .unit(event.unit())
                .effectiveTime(event.effectiveTime())
                .processedAt(Instant.now())
                .build();

            repository.save(entity);
            persistedCounter.increment();
            log.info("Successfully persisted event {} for patient {}", event.eventId(), event.patientId());
        });
    }
}