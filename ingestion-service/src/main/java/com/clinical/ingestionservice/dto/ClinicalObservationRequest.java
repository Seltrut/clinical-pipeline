package com.clinical.ingestionservice.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Instant;

public record ClinicalObservationRequest(
    @NotBlank(message = "Event ID cannot be blank") 
    String eventId,

    @NotBlank(message = "Patient ID cannot be blank") 
    String patientId,

    @NotBlank(message = "Clinical code is required") 
    String code,

    @NotBlank(message = "Display description is required") 
    String display,

    @NotNull(message = "Observation value is required") 
    Double value,

    @NotBlank(message = "Measurement unit is required") 
    String unit,

    @NotNull(message = "Effective timestamp is required") 
    Instant effectiveTime
) {}