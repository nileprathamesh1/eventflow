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

    public boolean isDuplicate(String eventId) {
        String key = "dedup:" + eventId;
        Boolean isNew = redisTemplate.opsForValue().setIfAbsent(key, "1", Duration.ofHours(24));
        return Boolean.FALSE.equals(isNew);
    }

    public boolean isRateLimited(String source) {
        String key = "rate:" + source;
        Long count = redisTemplate.opsForValue().increment(key);

        if (count != null && count == 1) {
            redisTemplate.expire(key, Duration.ofSeconds(windowSeconds));
        }

        return count != null && count > maxRequests;
    }
}
