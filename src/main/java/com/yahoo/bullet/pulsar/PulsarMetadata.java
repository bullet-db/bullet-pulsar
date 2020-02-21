/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pulsar;

import com.yahoo.bullet.pubsub.Metadata;
import lombok.Getter;

import java.io.Serializable;

public class PulsarMetadata extends Metadata implements Serializable {
    private static final long serialVersionUID = -4945699518421610051L;

    @Getter
    private String topicName;

    /**
     * Constructor that takes a {@link Metadata} to wrap and a topic name for the Pulsar topic.
     *
     * @param metadata The non-null metadata to wrap.
     * @param topicName The topic name for Pulsar.
     */
    public PulsarMetadata(Metadata metadata, String topicName) {
        super(metadata.getSignal(), metadata.getContent());
        this.topicName = topicName;
    }

    /**
     * Constructor.
     *
     * @param topicName The topic name for Pulsar.
     */
    public PulsarMetadata(String topicName) {
        this.topicName = topicName;
    }
}
