package com.clinical.consumerservice.dto;

import java.time.Instant;

public record ClinicalObservationEvent(
    String eventId,
    String patientId,
    String code,
    String display,
    Double value,
    String unit,
    Instant effectiveTime
) {}