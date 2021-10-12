/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pulsar;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.pubsub.PubSub;
import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.Publisher;
import com.yahoo.bullet.pubsub.Subscriber;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class PulsarPubSub extends PubSub {
    // Exposed for testing
    @Getter(AccessLevel.PACKAGE)
    @Setter(AccessLevel.PACKAGE)
    private SharedPulsarClient sharedPulsarClient;

    private String queryTopicName;              // QUERY_SUBMISSION
    private String responseTopicName;           // QUERY_SUBMISSION
    private List<String> queryTopicNames;       // QUERY_PROCESSING

    @Getter(AccessLevel.PACKAGE)
    private Map<String, Object> producerConf;

    @Getter(AccessLevel.PACKAGE)
    private Map<String, Object> consumerConf;

    /**
     * Creates a PulsarPubSub from a {@link BulletConfig}.
     *
     * @param pubSubConfig The {@link BulletConfig} from which to load settings.
     * @throws PubSubException If there is an error building the {@link PulsarClient}.
     */
    public PulsarPubSub(BulletConfig pubSubConfig) throws PubSubException {
        super(pubSubConfig);
        // Copy settings from pubSubConfig.
        config = new PulsarConfig(pubSubConfig);
        initialize();
    }

    @Override
    public void switchContext(Context context, BulletConfig config) throws PubSubException {
        super.switchContext(context, config);
        initialize();
    }

    /**
     * Does not close sharedPulsarClient (even if it does exist) nor should it have to.
     */
    private void initialize() {
        queryTopicName = config.getAs(PulsarConfig.PULSAR_REQUEST_TOPIC_NAME, String.class);
        responseTopicName = config.getAs(PulsarConfig.PULSAR_RESPONSE_TOPIC_NAME, String.class);
        queryTopicNames = config.getAs(PulsarConfig.PULSAR_REQUEST_TOPIC_NAMES, List.class);

        Map<String, Object> clientConf = config.getAllWithPrefix(Optional.empty(), PulsarConfig.PULSAR_CLIENT_NAMESPACE, true);
        producerConf = config.getAllWithPrefix(Optional.empty(), PulsarConfig.PULSAR_PRODUCER_NAMESPACE, true);
        consumerConf = config.getAllWithPrefix(Optional.empty(), PulsarConfig.PULSAR_CONSUMER_NAMESPACE, true);

        ClientBuilder clientBuilder = PulsarClient.builder().loadConf(clientConf);
        if (config.getAs(PulsarConfig.PULSAR_AUTH_ENABLE, Boolean.class)) {
            String authPluginClassName = config.getAs(PulsarConfig.PULSAR_AUTH_PLUGIN_CLASS_NAME, String.class);
            String authParamsString = config.getAs(PulsarConfig.PULSAR_AUTH_PARAMS_STRING, String.class);
            sharedPulsarClient = new SharedPulsarClient(clientBuilder, authPluginClassName, authParamsString);
        } else {
            sharedPulsarClient = new SharedPulsarClient(clientBuilder);
        }
    }

    @Override
    public Publisher getPublisher() throws PubSubException {
        if (context == Context.QUERY_PROCESSING) {
            return new PulsarResponsePublisher(sharedPulsarClient, producerConf);
        }
        return new PulsarQueryPublisher(sharedPulsarClient, producerConf, queryTopicName, responseTopicName);
    }

    @Override
    public List<Publisher> getPublishers(int n) throws PubSubException {
        List<Publisher> publishers = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            publishers.add(getPublisher());
        }
        return publishers;
    }

    @Override
    public Subscriber getSubscriber() throws PubSubException {
        Number maxUncommittedMessages = getRequiredConfig(Number.class, PulsarConfig.PULSAR_MAX_UNCOMMITTED_MESSAGES);
        if (context == Context.QUERY_PROCESSING) {
            return new PulsarSubscriber(sharedPulsarClient, consumerConf, queryTopicNames, maxUncommittedMessages.intValue());
        }
        return new PulsarSubscriber(sharedPulsarClient, consumerConf, Collections.singletonList(responseTopicName), maxUncommittedMessages.intValue());
    }

    @Override
    public List<Subscriber> getSubscribers(int n) throws PubSubException {
        List<Subscriber> subscribers = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            subscribers.add(getSubscriber());
        }
        return subscribers;
    }
}
