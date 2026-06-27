package com.eventflow.consumer;

import com.eventflow.dto.EventMessage;
import com.eventflow.model.Event;
import com.eventflow.repository.EventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Slf4j
@Component
public class EventConsumer {

    private final EventRepository eventRepository;
    private final ObjectMapper objectMapper;
    private final Counter processedCounter;
    private final Counter failedCounter;

    public EventConsumer(EventRepository eventRepository,
                         ObjectMapper objectMapper,
                         MeterRegistry meterRegistry) {
        this.eventRepository = eventRepository;
        this.objectMapper = objectMapper;
        // Metrics counters — visible at /actuator/prometheus
        this.processedCounter = Counter.builder("eventflow.events.processed")
                .description("Total events successfully processed")
                .register(meterRegistry);
        this.failedCounter = Counter.builder("eventflow.events.failed")
                .description("Total events that failed processing")
                .register(meterRegistry);
    }

    @KafkaListener(topics = "${eventflow.kafka.topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void consume(EventMessage message, @Header(KafkaHeaders.RECEIVED_PARTITION) int partition) {
        log.info("Consuming event {} from partition {}", message.getEventId(), partition);

        String payloadJson;
        try {
            payloadJson = objectMapper.writeValueAsString(message.getPayload());
        } catch (Exception e) {
            failedCounter.increment();
            // Throwing here triggers the DefaultErrorHandler — it will retry twice then send to DLT
            throw new RuntimeException("Failed to serialize payload for event: " + message.getEventId(), e);
        }

        Event event = Event.builder()
                .eventId(message.getEventId())
                .type(message.getType())
                .source(message.getSource())
                .payload(payloadJson)
                .receivedAt(message.getTimestamp())
                .processedAt(Instant.now())
                .status(Event.EventStatus.PROCESSED)
                .build();

        eventRepository.save(event);
        processedCounter.increment();
        log.info("Persisted event {}", message.getEventId());
    }

    /*
     * DLT listener — events land here after exhausting retries.
     * For now we just log them, but in production you'd alert on this
     * or store them separately for manual review.
     */
    @KafkaListener(topics = "${eventflow.kafka.dlt}", groupId = "${spring.kafka.consumer.group-id}-dlt")
    public void consumeFromDlt(EventMessage message) {
        log.error("Event {} landed in DLT — manual intervention required. Source: {}, Type: {}",
                message.getEventId(), message.getSource(), message.getType());
        failedCounter.increment();
    }
}
