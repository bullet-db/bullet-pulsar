/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pulsar;

import com.yahoo.bullet.common.SerializerDeserializer;
import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.pubsub.Publisher;
import lombok.AccessLevel;
import lombok.Getter;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.HashMap;
import java.util.Map;

public class PulsarResponsePublisher implements Publisher {
    // Exposed for testing
    @Getter(AccessLevel.PACKAGE)
    private SharedPulsarClient sharedPulsarClient;
    private ProducerBuilder<byte[]> builder;

    @Getter(AccessLevel.PACKAGE)
    private Map<String, Producer<byte[]>> producers = new HashMap<>();

    /**
     *  Creates a PulsarResponsePublisher from a {@link SharedPulsarClient} and appropriate configuration.
     *
     * @param sharedPulsarClient The SharedPulsarClient used to get a PulsarClient to create Producers from.
     * @param producerConf The configuration to create Pulsar producers with.
     * @throws PubSubException if there's an error getting the shared PulsarClient.
     */
    public PulsarResponsePublisher(SharedPulsarClient sharedPulsarClient, Map<String, Object> producerConf) throws PubSubException {
        this.sharedPulsarClient = sharedPulsarClient;
        this.builder = sharedPulsarClient.getPulsarClient().newProducer().loadConf(producerConf);
    }

    @Override
    public PubSubMessage send(PubSubMessage pubSubMessage) throws PubSubException {
        String responseTopicName = ((PulsarMetadata) pubSubMessage.getMetadata()).getTopicName();
        Producer<byte[]> producer = getProducer(responseTopicName);

        producer.sendAsync(SerializerDeserializer.toBytes(pubSubMessage));
        return pubSubMessage;
    }

    @Override
    public void close() {
        if (sharedPulsarClient != null) {
            producers.values().forEach(Producer::closeAsync);
            sharedPulsarClient.close();
            sharedPulsarClient = null;
        }
    }

    private Producer<byte[]> getProducer(String responseTopicName) throws PubSubException {
        if (!producers.containsKey(responseTopicName)) {
            try {
                producers.put(responseTopicName, builder.clone().topic(responseTopicName).create());
            } catch (PulsarClientException e) {
                throw new PubSubException("Could not create producer.", e);
            }
        }
        return producers.get(responseTopicName);
    }
}
