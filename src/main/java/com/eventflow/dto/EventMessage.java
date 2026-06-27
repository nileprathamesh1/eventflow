package com.eventflow.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EventMessage {

    private String eventId;
    private String type;
    private String source;
    private Object payload;
    private Instant timestamp;
}
