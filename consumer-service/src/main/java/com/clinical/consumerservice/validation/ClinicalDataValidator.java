package com.clinical.consumerservice.validation;

import com.clinical.consumerservice.dto.ClinicalObservationEvent;
import com.clinical.consumerservice.exception.InvalidClinicalDataException;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class ClinicalDataValidator {

    private record Range(double min, double max, String expectedUnit) {}

    // Standard LOINC code physiological bounds
    private static final Map<String, Range> LOINC_BOUNDS = Map.of(
        "8480-6", new Range(20.0, 350.0, "mmHg"),  // Systolic Blood Pressure
        "8462-4", new Range(10.0, 250.0, "mmHg"),  // Diastolic Blood Pressure
        "8478-0", new Range(15.0, 280.0, "mmHg"),  // Mean Blood Pressure
        "8867-4", new Range(10.0, 300.0, "/min"),  // Heart Rate
        "9279-1", new Range(2.0, 120.0, "/min"),   // Respiratory Rate
        "8310-5", new Range(25.0, 45.0, "Cel"),    // Body Temperature (Celsius)
        "2708-6", new Range(30.0, 100.0, "%"),     // Oxygen Saturation (SpO2)
        "29463-7", new Range(0.3, 500.0, "kg"),    // Body Weight
        "8302-2", new Range(20.0, 260.0, "cm"),    // Body Height
        "39156-5", new Range(5.0, 150.0, "kg/m2")  // Body Mass Index (BMI)
    );

    /**
     * Validates the clinical observation event against predefined physiological bounds.
     *
     * @param event the clinical observation event to validate
     * @throws InvalidClinicalDataException if the event contains invalid data
     */
    public void validate(ClinicalObservationEvent event) {
        if (event.value() == null || event.value().isNaN() || event.value().isInfinite()) {
            throw new InvalidClinicalDataException("Observation value must be a finite number");
        }

        Range bounds = LOINC_BOUNDS.get(event.code());
        if (bounds != null) {
            if (event.value() < bounds.min() || event.value() > bounds.max()) {
                throw new InvalidClinicalDataException(String.format(
                    "Physiologically impossible value %.2f %s for LOINC code %s (%s). Expected range: [%.1f - %.1f]",
                    event.value(), event.unit(), event.code(), event.display(), bounds.min(), bounds.max()
                ));
            }
        } else if (event.value() < 0.0) {
            // Fallback baseline rule for unmapped observation codes
            throw new InvalidClinicalDataException(
                "Negative observation value not permitted for code " + event.code() + ": " + event.value()
            );
        }
    }
}