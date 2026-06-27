package com.eventflow.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EventRequest {

    @NotBlank(message = "Event type is required")
    private String type;

    // Who is sending this event — used for rate limiting and deduplication
    @NotBlank(message = "Source is required")
    private String source;

    @NotNull(message = "Payload cannot be null")
    private Object payload;
}
