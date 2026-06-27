package com.eventflow.controller;

import com.eventflow.dto.EventRequest;
import com.eventflow.model.Event;
import com.eventflow.repository.EventRepository;
import com.eventflow.service.EventService;
import io.micrometer.core.annotation.Timed;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/events")
@RequiredArgsConstructor
public class EventController {

    private final EventService eventService;
    private final EventRepository eventRepository;

    // @Timed generates p50/p95/p99 latency histograms for this endpoint in Prometheus
    @Timed(value = "eventflow.publish.latency", description = "Latency of event publish endpoint", percentiles = {0.5, 0.95, 0.99})
    @PostMapping
    public ResponseEntity<Map<String, String>> publish(@Valid @RequestBody EventRequest request) {
        String eventId = eventService.publish(request);
        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(Map.of("eventId", eventId, "status", "queued"));
    }

    @GetMapping
    public ResponseEntity<List<Event>> getAllEvents() {
        return ResponseEntity.ok(eventRepository.findAll());
    }

    @GetMapping("/source/{source}")
    public ResponseEntity<List<Event>> getBySource(@PathVariable String source) {
        return ResponseEntity.ok(eventRepository.findBySource(source));
    }

    @GetMapping("/{eventId}")
    public ResponseEntity<Event> getById(@PathVariable String eventId) {
        return eventRepository.findById(eventId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }
}
