package com.eventflow.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class RedisServiceTest {

    @Mock
    private StringRedisTemplate redisTemplate;

    @Mock
    private ValueOperations<String, String> valueOps;

    @InjectMocks
    private RedisService redisService;

    @BeforeEach
    void setUp() {
        when(redisTemplate.opsForValue()).thenReturn(valueOps);
        ReflectionTestUtils.setField(redisService, "maxRequests", 10);
        ReflectionTestUtils.setField(redisService, "windowSeconds", 60);
    }

    @Test
    void isDuplicate_returnsFalse_forNewEventId() {
        when(valueOps.setIfAbsent(anyString(), eq("1"), any(Duration.class))).thenReturn(true);
        assertThat(redisService.isDuplicate("new-event-id")).isFalse();
    }

    @Test
    void isDuplicate_returnsTrue_forSeenEventId() {
        when(valueOps.setIfAbsent(anyString(), eq("1"), any(Duration.class))).thenReturn(false);
        assertThat(redisService.isDuplicate("seen-event-id")).isTrue();
    }

    @Test
    void isRateLimited_returnsFalse_whenUnderLimit() {
        when(valueOps.increment("rate:service-a")).thenReturn(5L);
        assertThat(redisService.isRateLimited("service-a")).isFalse();
    }

    @Test
    void isRateLimited_returnsTrue_whenLimitExceeded() {
        when(valueOps.increment("rate:service-a")).thenReturn(11L);
        assertThat(redisService.isRateLimited("service-a")).isTrue();
    }

    @Test
    void isRateLimited_setsTTL_onFirstRequestInWindow() {
        when(valueOps.increment("rate:service-a")).thenReturn(1L);
        redisService.isRateLimited("service-a");
        verify(redisTemplate).expire(eq("rate:service-a"), eq(Duration.ofSeconds(60)));
    }

    @Test
    void isRateLimited_doesNotSetTTL_onSubsequentRequests() {
        when(valueOps.increment("rate:service-a")).thenReturn(5L);
        redisService.isRateLimited("service-a");
        verify(redisTemplate, never()).expire(anyString(), any(Duration.class));
    }
}
