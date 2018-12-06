/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pulsar;

import com.yahoo.bullet.common.SerializerDeserializer;
import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.pubsub.Publisher;
import lombok.AccessLevel;
import lombok.Getter;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.Map;

public class PulsarQueryPublisher implements Publisher {
    // Exposed for testing
    @Getter(AccessLevel.PACKAGE)
    private SharedPulsarClient sharedPulsarClient;
    private String responseTopicName;
    private Producer<byte[]> producer;

    /**
     * Creates a PulsarQueryPublisher from a {@link SharedPulsarClient} and appropriate configuration.
     *
     * @param sharedPulsarClient The SharedPulsarClient used to get a PulsarClient to create a Producer from.
     * @param producerConf The configuration to create a Pulsar producer with.
     * @param queryTopicName The name of the topic to publish messages to.
     * @param responseTopicName The name of the topic that corresponding PulsarSubscribers will consume messages from.
     * @throws PubSubException if there's an error creating the Pulsar producer.
     */
    public PulsarQueryPublisher(SharedPulsarClient sharedPulsarClient, Map<String, Object> producerConf, String queryTopicName, String responseTopicName) throws PubSubException {
        this.sharedPulsarClient = sharedPulsarClient;
        this.responseTopicName = responseTopicName;
        try {
            producer = sharedPulsarClient.getPulsarClient().newProducer().loadConf(producerConf).topic(queryTopicName).create();
        } catch (PulsarClientException e) {
            throw new PubSubException("Could not create producer.", e);
        }
    }

    @Override
    public void send(PubSubMessage pubSubMessage) {
        if (!pubSubMessage.hasMetadata()) {
            pubSubMessage.setMetadata(new Metadata());
        }
        pubSubMessage.getMetadata().setContent(responseTopicName);
        producer.sendAsync(SerializerDeserializer.toBytes(pubSubMessage));
    }

    @Override
    public void close() {
        if (sharedPulsarClient != null) {
            producer.closeAsync();
            sharedPulsarClient.close();
            sharedPulsarClient = null;
        }
    }
}
