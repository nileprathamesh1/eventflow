package com.eventflow.service;

import com.eventflow.dto.EventMessage;
import com.eventflow.dto.EventRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class EventService {

    private final KafkaTemplate<String, EventMessage> kafkaTemplate;
    private final RedisService redisService;

    @Value("${eventflow.kafka.topic}")
    private String topic;

    public String publish(EventRequest request) {
        if (redisService.isRateLimited(request.getSource())) {
            throw new RateLimitExceededException("Rate limit exceeded for source: " + request.getSource());
        }

        String eventId = UUID.randomUUID().toString();

        if (redisService.isDuplicate(eventId)) {
            // UUID collision is astronomically unlikely, but good practice to handle
            log.warn("Duplicate event ID generated: {}", eventId);
            throw new IllegalStateException("Duplicate event ID, please retry");
        }

        EventMessage message = EventMessage.builder()
                .eventId(eventId)
                .type(request.getType())
                .source(request.getSource())
                .payload(request.getPayload())
                .timestamp(Instant.now())
                .build();

        // Keying by source ensures events from the same source land on the same partition,
        // preserving ordering per source
        kafkaTemplate.send(topic, request.getSource(), message);
        log.info("Published event {} of type {} from {}", eventId, request.getType(), request.getSource());

        return eventId;
    }

    public static class RateLimitExceededException extends RuntimeException {
        public RateLimitExceededException(String message) {
            super(message);
        }
    }
}
