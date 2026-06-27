package com.eventflow.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Entity
@Table(name = "events")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Event {

    @Id
    @Column(name = "event_id")
    private String eventId;

    @Column(nullable = false)
    private String type;

    @Column(nullable = false)
    private String source;

    // Storing payload as JSON string for flexibility
    @Column(columnDefinition = "TEXT")
    private String payload;

    @Column(nullable = false)
    private Instant receivedAt;

    @Column(nullable = false)
    private Instant processedAt;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private EventStatus status;

    public enum EventStatus {
        PROCESSED, FAILED
    }
}
