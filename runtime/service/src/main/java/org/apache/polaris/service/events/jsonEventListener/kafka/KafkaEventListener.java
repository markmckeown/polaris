/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.polaris.service.events.jsonEventListener.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.common.annotation.Identifier;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.UUIDSerializer;
import org.apache.polaris.service.events.EventAttributes;
import org.apache.polaris.service.events.PolarisEvent;
import org.apache.polaris.service.events.listeners.PolarisEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Properties;
import java.util.UUID;

/**
 * Event Listener that pushes Apache Polaris Table events to a Kafka topic.
 */
@ApplicationScoped
@Identifier("kafka")
public class KafkaEventListener implements PolarisEventListener {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaEventListener.class);
  private KafkaProducer<UUID, String> producer;
  private final String topic;
  private final boolean synchronousMode;
  private final ObjectMapper objectMapper;
  private final String bootStrapServers;

  @Inject
  public KafkaEventListener(KafkaEventListenerConfiguration configuration,
                            ObjectMapper objectMapper) {
    this.bootStrapServers = configuration.bootstrapServers();
    this.synchronousMode = configuration.synchronousMode();
    this.topic = configuration.topic();
    this.objectMapper = objectMapper;
  }

  @PostConstruct
  void start() {
    LOGGER.info("Starting KafkaEventListener");
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootStrapServers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, UUIDSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    this.producer = new KafkaProducer<>(props);
  }

  @PreDestroy
  void shutdown() {
    if (producer != null) {
      producer.close();
      producer = null;
    }
  }

  @Override
  public void onEvent(PolarisEvent event) {
    LOGGER.info("Got event '{}', sending to topic '{}' using synchronous mode: '{}'.",
        event.type(), topic, synchronousMode);

    HashMap<String, Object> properties = new HashMap<>();
    properties.put("event_type", event.type().name());
    event
        .attributes()
        .get(EventAttributes.TABLE_IDENTIFIER)
        .map(TableIdentifier::toString)
        .ifPresent(id -> properties.put("table_identifier", id));
    event
        .attributes()
        .get(EventAttributes.VIEW_IDENTIFIER)
        .map(TableIdentifier::toString)
        .ifPresent(id -> properties.put("view_identifier", id));
    event
        .attributes()
        .get(EventAttributes.NAMESPACE_NAME)
        .ifPresent(id -> properties.put("namespace_name", id));
    event
        .attributes()
        .get(EventAttributes.CATALOG_NAME)
        .ifPresent(id -> properties.put("catalog_name", id));
    properties.put("realm_id", event.metadata().realmId());
    event
        .metadata()
        .user()
        .ifPresent(
            p -> {
              properties.put("principal", p.getName());
            });
    event.metadata().requestId().ifPresent(id -> properties.put("request_id", id));

    String eventAsJson;
    try {
      eventAsJson = objectMapper.writeValueAsString(properties);
    } catch (JsonProcessingException e) {
      LOGGER.error("Error processing event into JSON string: ", e);
      return;
    }

    ProducerRecord<UUID, String> record = new ProducerRecord<>(topic, event.metadata().eventId(), eventAsJson);
    if (synchronousMode) {
      try {
        RecordMetadata recordMetadata = producer.send(record).get();
        LOGGER.info("Sent PolarisEvent {} to Kafka topic {} at offset {}", event.type(), topic, recordMetadata.offset());
      } catch (Exception exception) {
        LOGGER.error("Failed to send PolarisEvent to Kafka topic {}", topic, exception);
      }
    } else {
      var unused = producer.send(record, (metadata, exception) -> {
        if (exception != null) {
          LOGGER.error("Failed to send PolarisEvent to Kafka topic {}", topic, exception);
        } else {
          LOGGER.info("Sent PolarisEvent {} to Kafka topic {}", event.type(), topic);
        }
      });
    }
  }
}