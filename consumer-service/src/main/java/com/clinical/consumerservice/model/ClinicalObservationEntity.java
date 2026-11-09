package com.clinical.consumerservice.model;

import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import jakarta.persistence.Index;
import jakarta.persistence.Id;
import jakarta.persistence.Column;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import lombok.Getter;
import lombok.Setter;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import lombok.Builder;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(
    name = "clinical_observations",
    indexes = {
        @Index(name = "idx_patient_id", columnList = "patientId"),
        @Index(name = "idx_event_id", columnList = "eventId", unique = true)
    }
)
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ClinicalObservationEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(nullable = false, unique = true)
    private String eventId;

    @Column(nullable = false)
    private String patientId;

    @Column(nullable = false)
    private String code;

    private String display;

    @Column(nullable = false)
    private Double value;

    private String unit;

    @Column(nullable = false)
    private Instant effectiveTime;

    @Column(nullable = false)
    private Instant processedAt;
}