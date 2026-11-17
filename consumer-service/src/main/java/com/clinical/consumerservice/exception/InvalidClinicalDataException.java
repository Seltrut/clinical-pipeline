package com.clinical.consumerservice.exception;

public class InvalidClinicalDataException extends RuntimeException {
    public InvalidClinicalDataException(String message) {
        super(message);
    }
}