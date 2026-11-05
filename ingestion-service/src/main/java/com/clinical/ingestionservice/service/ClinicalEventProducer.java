package com.clinical.ingestionservice.service;

import com.clinical.ingestionservice.dto.ClinicalObservationRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class ClinicalEventProducer {

    private final KafkaTemplate<String, ClinicalObservationRequest> kafkaTemplate;

    @Value("${app.kafka.topics.clinical-events}")
    private String topicName;

    public CompletableFuture<SendResult<String, ClinicalObservationRequest>> sendEvent(ClinicalObservationRequest event) {
        // Using patientId as the partition key ensures strict chronological order per patient
        return kafkaTemplate.send(topicName, event.patientId(), event)
            .whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("Failed to publish event {}: {}", event.eventId(), ex.getMessage());
                } else {
                    log.info("Published event {} to partition {}", 
                        event.eventId(), result.getRecordMetadata().partition());
                }
            });
    }
}