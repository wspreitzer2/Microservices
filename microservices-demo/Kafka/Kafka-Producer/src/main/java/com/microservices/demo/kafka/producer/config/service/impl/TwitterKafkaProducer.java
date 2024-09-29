package com.microservices.demo.kafka.producer.config.service.impl;

import com.microservices.demo.kafka.avro.model.TwitterAvroModel;
import com.microservices.demo.kafka.producer.config.service.KafkaProducer;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import jakarta.annotation.PreDestroy;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

@Service
public class TwitterKafkaProducer implements KafkaProducer<Long, TwitterAvroModel> {
    private static final Logger LOG = LoggerFactory.getLogger(TwitterKafkaProducer.class);
    private KafkaTemplate<Long, TwitterAvroModel> kafkaTemplate;

    public TwitterKafkaProducer(KafkaTemplate<Long, TwitterAvroModel> template) {
        this.kafkaTemplate = template;
    }

    @Override
    public void send(String topicName, Long key, TwitterAvroModel message) {
        LOG.debug("Received new Metadata, Topic: {}; Partition: {}; Offset: {}; Timestamp: {}, at Time{}.",
                topicName,
                topicName,
                topicName,
                topicName,
                System.nanoTime());
        final CompletableFuture<SendResult<Long, TwitterAvroModel>> completableKafkaFuture = (CompletableFuture<SendResult<Long, TwitterAvroModel>>) kafkaTemplate.send(topicName, key, message);
        addCallback(topicName, message, completableKafkaFuture);
    }

    private void addCallback(final String topicName, final TwitterAvroModel message,
                             final CompletableFuture<SendResult<Long, TwitterAvroModel>> result) {
        result.whenComplete((r, throwable) -> {
            if (throwable != null) {
                LOG.error("Error received new Metadata, Topic: {}; Partition: {}; Offset: {}; Timestamp: {}, at Time{}.",
                        topicName,
                        topicName,
                        topicName,
                        topicName,
                        System.nanoTime());
            } else {
                final RecordMetadata recordMetadata = r.getRecordMetadata();
                LOG.debug("Received new Metadata, Topic: {}; Partition: {}; Offset: {}; Timestamp: {}, at Time{}.",
                        recordMetadata.topic(),
                        recordMetadata.partition(),
                        recordMetadata.offset(),
                        recordMetadata.timestamp(),
                        System.nanoTime());
            }
        });
    }

    @PreDestroy
    public void close() {
        if (kafkaTemplate != null) {
            LOG.info("Closing kafka producer!");
            kafkaTemplate.destroy();
        }
    }
}
