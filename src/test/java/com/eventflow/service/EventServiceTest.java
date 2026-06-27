package com.eventflow.service;

import com.eventflow.dto.EventMessage;
import com.eventflow.dto.EventRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class EventServiceTest {

    @Mock
    private KafkaTemplate<String, EventMessage> kafkaTemplate;

    @Mock
    private RedisService redisService;

    @InjectMocks
    private EventService eventService;

    @Test
    void publish_returnsEventId_onSuccess() {
        ReflectionTestUtils.setField(eventService, "topic", "events");
        when(redisService.isRateLimited(any())).thenReturn(false);
        when(redisService.isDuplicate(any())).thenReturn(false);

        EventRequest request = EventRequest.builder()
                .type("user.signup")
                .source("auth-service")
                .payload("test-payload")
                .build();

        String eventId = eventService.publish(request);

        assertThat(eventId).isNotBlank();
    }

    @Test
    void publish_sendsMessageToKafka_withSourceAsKey() {
        ReflectionTestUtils.setField(eventService, "topic", "events");
        when(redisService.isRateLimited(any())).thenReturn(false);
        when(redisService.isDuplicate(any())).thenReturn(false);

        EventRequest request = EventRequest.builder()
                .type("order.placed")
                .source("order-service")
                .payload("order-payload")
                .build();

        eventService.publish(request);

        // Verify the message was keyed by source — ensures ordering per source in Kafka
        ArgumentCaptor<EventMessage> messageCaptor = ArgumentCaptor.forClass(EventMessage.class);
        verify(kafkaTemplate).send(eq("events"), eq("order-service"), messageCaptor.capture());

        EventMessage sent = messageCaptor.getValue();
        assertThat(sent.getType()).isEqualTo("order.placed");
        assertThat(sent.getSource()).isEqualTo("order-service");
    }

    @Test
    void publish_throwsRateLimitException_whenSourceIsRateLimited() {
        when(redisService.isRateLimited("throttled-service")).thenReturn(true);

        EventRequest request = EventRequest.builder()
                .type("event.type")
                .source("throttled-service")
                .payload("payload")
                .build();

        assertThatThrownBy(() -> eventService.publish(request))
                .isInstanceOf(EventService.RateLimitExceededException.class)
                .hasMessageContaining("throttled-service");

        // Kafka should not be called if rate limited
        verifyNoInteractions(kafkaTemplate);
    }

    @Test
    void publish_doesNotCallKafka_whenRateLimited() {
        when(redisService.isRateLimited(any())).thenReturn(true);

        EventRequest request = EventRequest.builder()
                .type("event.type")
                .source("any-source")
                .payload("payload")
                .build();

        try {
            eventService.publish(request);
        } catch (EventService.RateLimitExceededException ignored) {}

        verifyNoInteractions(kafkaTemplate);
    }
}
