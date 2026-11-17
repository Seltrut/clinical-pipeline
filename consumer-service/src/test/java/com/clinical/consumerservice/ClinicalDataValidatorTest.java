package com.clinical.consumerservice;

import com.clinical.consumerservice.dto.ClinicalObservationEvent;
import com.clinical.consumerservice.exception.InvalidClinicalDataException;
import com.clinical.consumerservice.validation.ClinicalDataValidator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ClinicalDataValidatorTest {

    private ClinicalDataValidator validator;

    @BeforeEach
    void setUp() {
        validator = new ClinicalDataValidator();
    }

    @ParameterizedTest(name = "Valid {1} ({0}): {2} {3}")
    @CsvSource({
        "8480-6, Systolic BP, 120.0, mmHg",
        "8480-6, Systolic BP Min Boundary, 20.0, mmHg",
        "8480-6, Systolic BP Max Boundary, 350.0, mmHg",
        "8462-4, Diastolic BP, 80.0, mmHg",
        "8867-4, Heart Rate, 72.0, /min",
        "9279-1, Respiratory Rate, 16.0, /min",
        "8310-5, Body Temperature, 37.0, Cel",
        "2708-6, Arterial SpO2, 98.0, %",
        "59408-5, Pulse Oximetry SpO2, 100.0, %",
        "29463-7, Body Weight, 75.5, kg",
        "8302-2, Body Height, 178.0, cm",
        "39156-5, BMI, 23.8, kg/m2",
        "9999-9, Unmapped Custom Lab Code, 0.0, mg/dL"
    })
    @DisplayName("Valid clinical observations within bounds pass validation")
    void shouldPassValidObservations(String code, String display, double value, String unit) {
        var event = new ClinicalObservationEvent(
            "evt-1", "pat-1", code, display, value, unit, Instant.now()
        );

        assertThatCode(() -> validator.validate(event)).doesNotThrowAnyException();
    }

    @ParameterizedTest(name = "Invalid {1} ({0}): {2} {3}")
    @CsvSource({
        "8480-6, Systolic BP Too Low, 19.9, mmHg",
        "8480-6, Systolic BP Too High, 350.1, mmHg",
        "8462-4, Diastolic BP Too Low, 5.0, mmHg",
        "8867-4, Heart Rate Too High, 305.0, /min",
        "8310-5, Body Temp Hypothermic Impossible, 20.0, Cel",
        "8310-5, Body Temp Hyperthermic Impossible, 48.0, Cel",
        "2708-6, SpO2 Above 100 Percent, 105.0, %",
        "9999-9, Unmapped Code Negative Value, -0.1, mg/dL"
    })
    @DisplayName("Out-of-bounds clinical observations throw InvalidClinicalDataException")
    void shouldRejectOutOfBoundsObservations(String code, String display, double value, String unit) {
        var event = new ClinicalObservationEvent(
            "evt-bad", "pat-1", code, display, value, unit, Instant.now()
        );

        assertThatThrownBy(() -> validator.validate(event))
            .isInstanceOf(InvalidClinicalDataException.class);
    }

    @ParameterizedTest
    @ValueSource(doubles = {Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY})
    @DisplayName("Non-finite floating point values (NaN, Infinity) are rejected")
    void shouldRejectNonFiniteValues(double nonFiniteValue) {
        var event = new ClinicalObservationEvent(
            "evt-nan", "pat-1", "8480-6", "Systolic BP", nonFiniteValue, "mmHg", Instant.now()
        );

        assertThatThrownBy(() -> validator.validate(event))
            .isInstanceOf(InvalidClinicalDataException.class)
            .hasMessageContaining("finite number");
    }

    @Test
    @DisplayName("Null observation value is rejected")
    void shouldRejectNullValue() {
        var event = new ClinicalObservationEvent(
            "evt-null", "pat-1", "8480-6", "Systolic BP", null, "mmHg", Instant.now()
        );

        assertThatThrownBy(() -> validator.validate(event))
            .isInstanceOf(InvalidClinicalDataException.class);
    }
}