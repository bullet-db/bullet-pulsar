/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pulsar;

import com.yahoo.bullet.pubsub.PubSubException;
import lombok.AccessLevel;
import lombok.Getter;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.concurrent.atomic.AtomicInteger;

public class SharedPulsarClient {
    // To be safe, we just make the reference volatile
    private volatile PulsarClient pulsarClient;

    // Exposed for testing
    @Getter(AccessLevel.PACKAGE)
    private AtomicInteger refCount = new AtomicInteger();

    private final ClientBuilder clientBuilder;
    private boolean authEnabled;
    private String authPluginClassName;
    private String authParamsString;

    /**
     * Creates a SharedPulsarClient from a {@link ClientBuilder}.
     *
     * @param clientBuilder The builder used to create a shared {@link PulsarClient}.
     */
    SharedPulsarClient(ClientBuilder clientBuilder) {
        this.clientBuilder = clientBuilder;
        this.authEnabled = false;
    }

    /**
     * Creates a SharedPulsarClient from a {@link ClientBuilder} and authentication details.
     *
     * @param clientBuilder The builder used to create a shared {@link PulsarClient}.
     * @param authPluginClassName The class name of the {@link org.apache.pulsar.client.api.Authentication}.
     * @param authParamsString The parameter string to create the authentication with.
     */
    SharedPulsarClient(ClientBuilder clientBuilder, String authPluginClassName, String authParamsString) {
        this.clientBuilder = clientBuilder;
        this.authEnabled = true;
        this.authPluginClassName = authPluginClassName;
        this.authParamsString = authParamsString;
    }

    /**
     * Gets the shared {@link PulsarClient}, creating it if it does not exist, and increments the reference count. Only
     * called in the constructors of the Publishers and Subscriber.
     *
     * @return A shared PulsarClient.
     */
    synchronized PulsarClient getPulsarClient() throws PubSubException {
        if (pulsarClient == null) {
            try {
                ClientBuilder builder = clientBuilder.clone();
                if (authEnabled) {
                    builder.authentication(authPluginClassName, authParamsString);
                }
                pulsarClient = builder.build();
            } catch (PulsarClientException e) {
                throw new PubSubException("Could not create PulsarClient.", e);
            }
        }
        refCount.incrementAndGet();
        return pulsarClient;
    }

    /**
     * Decrements the reference count and closes the shared PulsarClient if the count hits 0. Only called in the close
     * methods of the Pulsar publishers and subscriber.
     */
    void close() {
        // Each publisher/subscriber can only call close() once
        if (pulsarClient != null && refCount.decrementAndGet() == 0) {
            pulsarClient.closeAsync();
            pulsarClient = null;
        }
    }
}
