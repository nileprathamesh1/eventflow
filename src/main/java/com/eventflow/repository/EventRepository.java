package com.eventflow.repository;

import com.eventflow.model.Event;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface EventRepository extends JpaRepository<Event, String> {

    List<Event> findBySource(String source);

    List<Event> findByType(String type);
}
