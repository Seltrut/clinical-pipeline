package com.clinical.ingestionservice.controller;

import com.clinical.ingestionservice.dto.ClinicalObservationRequest;
import com.clinical.ingestionservice.service.ClinicalEventProducer;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/observations")
@RequiredArgsConstructor
public class ClinicalObservationController {

    private final ClinicalEventProducer producer;

    @PostMapping
    public ResponseEntity<Map<String, String>> ingestObservation(@Valid @RequestBody ClinicalObservationRequest request) {
        producer.sendEvent(request);
        return ResponseEntity.accepted().body(Map.of(
            "status", "ACCEPTED",
            "eventId", request.eventId()
        ));
    }
}