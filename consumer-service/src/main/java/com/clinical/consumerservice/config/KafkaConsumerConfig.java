package com.clinical.consumerservice.config;

import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import com.clinical.consumerservice.exception.InvalidClinicalDataException;

@Configuration
public class KafkaConsumerConfig {

    @Value("${app.kafka.topics.clinical-dlq}")
    private String dlqTopicName;

    @Bean
    public CommonErrorHandler errorHandler(KafkaTemplate<String, Object> kafkaTemplate) {
        // When retries are exhausted, route to our configured DLQ topic
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
            kafkaTemplate,
            (consumerRecord, exception) -> new TopicPartition(dlqTopicName, consumerRecord.partition())
        );

        // Retry 2 times with a 500ms delay between attempts before publishing to DLQ
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, new FixedBackOff(500L, 2L));
        errorHandler.addNotRetryableExceptions(InvalidClinicalDataException.class);

        return errorHandler;
    }
}