package com.eventflow.service;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;

@Service
@RequiredArgsConstructor
public class RedisService {

    private final StringRedisTemplate redisTemplate;

    @Value("${eventflow.rate-limit.max-requests}")
    private int maxRequests;

    @Value("${eventflow.rate-limit.window-seconds}")
    private int windowSeconds;

    /*
     * Deduplication: we store the eventId in Redis with a TTL of 24 hours.
     * If we see the same eventId again within that window, we reject it.
     * This prevents double-processing if a client retries a failed request.
     */
    public boolean isDuplicate(String eventId) {
        String key = "dedup:" + eventId;
        Boolean isNew = redisTemplate.opsForValue().setIfAbsent(key, "1", Duration.ofHours(24));
        return Boolean.FALSE.equals(isNew);
    }

    /*
     * Rate limiting: sliding window counter per source.
     * Each source gets a rolling window of `windowSeconds` seconds,
     * within which they can publish at most `maxRequests` events.
     */
    public boolean isRateLimited(String source) {
        String key = "rate:" + source;
        Long count = redisTemplate.opsForValue().increment(key);

        // Set TTL only on first request in the window
        if (count != null && count == 1) {
            redisTemplate.expire(key, Duration.ofSeconds(windowSeconds));
        }

        return count != null && count > maxRequests;
    }
}
