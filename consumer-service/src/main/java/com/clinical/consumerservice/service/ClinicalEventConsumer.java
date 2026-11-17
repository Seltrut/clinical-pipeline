package com.clinical.consumerservice.service;

import com.clinical.consumerservice.dto.ClinicalObservationEvent;
import com.clinical.consumerservice.exception.InvalidClinicalDataException;
import com.clinical.consumerservice.model.ClinicalObservationEntity;
import com.clinical.consumerservice.repository.ClinicalObservationRepository;
import com.clinical.consumerservice.validation.ClinicalDataValidator;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;

@Slf4j
@Service
@RequiredArgsConstructor
public class ClinicalEventConsumer {

    private final ClinicalObservationRepository repository;
    private final StringRedisTemplate redisTemplate;
    private final ClinicalDataValidator validator;

    private static final String IDEMPOTENCY_PREFIX = "processed_event:";
    private static final Duration TTL = Duration.ofHours(24);

    /*
        * Kafka listener for clinical observation events.
        * Ensures idempotent processing using Redis to track processed event IDs.
        * If an event has already been processed, it will be skipped to prevent duplicate database writes.
    */
    @KafkaListener(topics = "${app.kafka.topics.clinical-events}", groupId = "${spring.kafka.consumer.group-id}")
    public void processClinicalEvent(ClinicalObservationEvent event) {
        log.info("Recieved event {} for patient {}", event.eventId(), event.patientId());
        validator.validate(event);

        String redisKey = IDEMPOTENCY_PREFIX + event.eventId();

        // Atomic check-and-set in Redis (SETNX with 24-hour expiration)
        Boolean isNewEvent = redisTemplate.opsForValue().setIfAbsent(redisKey, "PROCESSED", TTL);

        if (Boolean.FALSE.equals(isNewEvent)) {
            log.warn("Duplicate event detected: {}. Skipping database write.", event.eventId());
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
        log.info("Successfully persisted event {} for patient {}", event.eventId(), event.patientId());
    }
}