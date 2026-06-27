package com.eventflow.integration;

import com.eventflow.dto.EventRequest;
import com.eventflow.repository.EventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import com.redis.testcontainers.RedisContainer;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/*
 * Full integration test — spins up real Kafka, PostgreSQL, and Redis via Testcontainers.
 * Tests the entire flow: REST API -> Kafka -> Consumer -> PostgreSQL.
 *
 * Testcontainers pulls Docker images and manages container lifecycle automatically.
 * No mocks here — this tests the actual wiring.
 */
@SpringBootTest
@AutoConfigureMockMvc
@Testcontainers
class EventFlowIntegrationTest {

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.5.0"));

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15-alpine")
            .withDatabaseName("eventflow")
            .withUsername("eventflow")
            .withPassword("eventflow123");

    @Container
    static RedisContainer redis = new RedisContainer(
            DockerImageName.parse("redis:7-alpine"));

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
    }

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private EventRepository eventRepository;

    @Test
    void publishEvent_isAccepted_andPersistedToDatabase() throws Exception {
        EventRequest request = EventRequest.builder()
                .type("user.signup")
                .source("auth-service")
                .payload("test-payload")
                .build();

        // Step 1: POST the event — should return 202 Accepted with an eventId
        String response = mockMvc.perform(post("/api/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.eventId").isNotEmpty())
                .andExpect(jsonPath("$.status").value("queued"))
                .andReturn()
                .getResponse()
                .getContentAsString();

        String eventId = objectMapper.readTree(response).get("eventId").asText();

        // Step 2: Wait for the Kafka consumer to process and persist the event
        // Awaitility polls until the condition is met or times out
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(eventRepository.findById(eventId)).isPresent();
            assertThat(eventRepository.findById(eventId).get().getSource()).isEqualTo("auth-service");
        });
    }

    @Test
    void publishEvent_returns429_whenRateLimitExceeded() throws Exception {
        EventRequest request = EventRequest.builder()
                .type("spam.event")
                .source("spammy-service")
                .payload("payload")
                .build();

        // Send 10 events (the limit)
        for (int i = 0; i < 10; i++) {
            mockMvc.perform(post("/api/events")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(request)))
                    .andExpect(status().isAccepted());
        }

        // 11th event should be rate limited
        mockMvc.perform(post("/api/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isTooManyRequests());
    }

    @Test
    void publishEvent_returns400_whenTypeIsMissing() throws Exception {
        String invalidRequest = """
                {
                    "source": "auth-service",
                    "payload": "some-payload"
                }
                """;

        mockMvc.perform(post("/api/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(invalidRequest))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").isNotEmpty());
    }
}
