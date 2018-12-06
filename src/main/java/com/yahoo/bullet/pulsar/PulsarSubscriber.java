/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pulsar;

import com.yahoo.bullet.common.SerializerDeserializer;
import com.yahoo.bullet.pubsub.BufferingSubscriber;
import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.PubSubMessage;
import lombok.AccessLevel;
import lombok.Getter;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class PulsarSubscriber extends BufferingSubscriber {
    // Exposed for testing
    @Getter(AccessLevel.PACKAGE)
    private SharedPulsarClient sharedPulsarClient;
    private Consumer<byte[]> consumer;

    /**
     * Creates a PulsarSubscriber from a {@link SharedPulsarClient} and appropriate configuration.
     *
     * @param sharedPulsarClient The SharedPulsarClient used to get a PulsarClient to create a Subscriber from.
     * @param consumerConf The configuration to create a Pulsar consumer with.
     * @param responseTopicNames The list of names of the topics to consume from.
     * @param maxUncommittedMessages The maximum number of messages that can be received before a commit is needed.
     * @throws PubSubException if there's an error creating the Pulsar consumer.
     */
    public PulsarSubscriber(SharedPulsarClient sharedPulsarClient, Map<String, Object> consumerConf, List<String> responseTopicNames, int maxUncommittedMessages) throws PubSubException {
        super(maxUncommittedMessages);
        this.sharedPulsarClient = sharedPulsarClient;
        try {
            consumer = sharedPulsarClient.getPulsarClient().newConsumer().loadConf(consumerConf).topics(responseTopicNames).subscribe();
        } catch (PulsarClientException e) {
            throw new PubSubException("Could not create consumer.", e);
        }
    }

    @Override
    protected List<PubSubMessage> getMessages() throws PubSubException {
        List<PubSubMessage> messages = new ArrayList<>();
        Message<byte[]> message;
        while ((message = getMessage()) != null) {
            messages.add(SerializerDeserializer.fromBytes(message.getData()));
            consumer.acknowledgeAsync(message);
        }
        return messages;
    }

    private Message<byte[]> getMessage() throws PubSubException {
        try {
            return consumer.receive(0, TimeUnit.MILLISECONDS);
        } catch (PulsarClientException e) {
            throw new PubSubException("Could not read from consumer.", e);
        }
    }

    @Override
    public void close() {
        if (sharedPulsarClient != null) {
            consumer.closeAsync();
            sharedPulsarClient.close();
            sharedPulsarClient = null;
        }
    }
}
