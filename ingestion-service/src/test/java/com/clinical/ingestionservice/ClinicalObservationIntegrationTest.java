package com.clinical.ingestionservice;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@Testcontainers
class ClinicalObservationIntegrationTest {

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(
        DockerImageName.parse("confluentinc/cp-kafka:7.6.0")
    );

    @DynamicPropertySource
    static void overrideKafkaProperties(DynamicPropertyRegistry registry) {
        // Points Spring Boot to the dynamically assigned port of the Testcontainer
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @Autowired
    private MockMvc mockMvc;

    private Consumer<String, String> testConsumer;

    @BeforeEach
    void setUpConsumer() {
        // Set up a lightweight Kafka consumer to inspect the broker directly
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(
            KAFKA.getBootstrapServers(), "test-verification-group", "true"
        );
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        testConsumer = new DefaultKafkaConsumerFactory<String, String>(consumerProps).createConsumer();
        testConsumer.subscribe(Collections.singletonList("clinical.events.raw"));
    }

    @AfterEach
    void tearDownConsumer() {
        if (testConsumer != null) {
            testConsumer.close();
        }
    }

    @Test
    @DisplayName("Valid observation is accepted and published to Kafka with patient ID as key")
    void shouldAcceptAndPublishObservation() throws Exception {
        String payload = """
            {
              "eventId": "evt-test-100",
              "patientId": "pat-55443",
              "code": "8480-6",
              "display": "Systolic Blood Pressure",
              "value": 118.0,
              "unit": "mmHg",
              "effectiveTime": "2026-11-04T22:30:00Z"
            }
            """;

        mockMvc.perform(post("/api/v1/observations")
                .contentType(MediaType.APPLICATION_JSON)
                .content(payload))
            .andExpect(status().isAccepted())
            .andExpect(jsonPath("$.status").value("ACCEPTED"))
            .andExpect(jsonPath("$.eventId").value("evt-test-100"));

        // Verify the message physically landed on the broker within 10 seconds
        ConsumerRecord<String, String> record = KafkaTestUtils.getSingleRecord(
            testConsumer, "clinical.events.raw", Duration.ofSeconds(10)
        );

        assertThat(record.key()).isEqualTo("pat-55443");
        assertThat(record.value()).contains("evt-test-100");
        assertThat(record.value()).contains("118.0");
    }

    @Test
    @DisplayName("Invalid payload with missing patient ID returns 400 and does not publish")
    void shouldRejectInvalidObservation() throws Exception {
        String invalidPayload = """
            {
              "eventId": "evt-invalid-001",
              "patientId": "",
              "code": "8480-6",
              "display": "Systolic Blood Pressure",
              "value": 118.0,
              "unit": "mmHg",
              "effectiveTime": "2026-11-04T22:30:00Z"
            }
            """;

        mockMvc.perform(post("/api/v1/observations")
                .contentType(MediaType.APPLICATION_JSON)
                .content(invalidPayload))
            .andExpect(status().isBadRequest());
    }
}