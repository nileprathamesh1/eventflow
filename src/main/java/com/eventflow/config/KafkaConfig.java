package com.eventflow.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.util.backoff.FixedBackOff;
import com.eventflow.dto.EventMessage;

@Configuration
public class KafkaConfig {

    @Value("${eventflow.kafka.topic}")
    private String topicName;

    @Value("${eventflow.kafka.dlt}")
    private String dltName;

    @Bean
    public NewTopic eventsTopic() {
        // 3 partitions so multiple consumer instances can process in parallel
        return TopicBuilder.name(topicName)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic deadLetterTopic() {
        // DLT gets a single partition - failed events are low volume
        return TopicBuilder.name(dltName)
                .partitions(1)
                .replicas(1)
                .build();
    }

    /*
     * Error handler for the Kafka listener container.
     * On failure: retry twice with a 2-second gap, then route to the DLT.
     * The DLT message includes headers with the original exception so you can debug later.
     */
    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, EventMessage> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate);
        FixedBackOff backOff = new FixedBackOff(2000L, 2);
        return new DefaultErrorHandler(recoverer, backOff);
    }
}
