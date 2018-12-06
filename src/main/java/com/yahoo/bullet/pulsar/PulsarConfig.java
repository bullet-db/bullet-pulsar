/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pulsar;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.Config;
import com.yahoo.bullet.common.Validator;
import com.yahoo.bullet.pubsub.PubSub;
import org.apache.pulsar.client.api.SubscriptionType;

import static com.yahoo.bullet.common.Validator.isImpliedBy;
import static java.util.function.Predicate.isEqual;

public class PulsarConfig extends BulletConfig {
    // PulsarPubSub configuration
    public static final String PULSAR_REQUEST_TOPIC_NAME = "bullet.pubsub.pulsar.request.topic.name";
    public static final String PULSAR_RESPONSE_TOPIC_NAME = "bullet.pubsub.pulsar.response.topic.name";
    public static final String PULSAR_RESPONSE_TOPIC_NAMES = "bullet.pubsub.pulsar.response.topic.names";
    public static final String PULSAR_MAX_UNCOMMITTED_MESSAGES = "bullet.pubsub.pulsar.subscriber.max.uncommitted.messages";

    // PulsarClient authentication configuration
    public static final String PULSAR_AUTH_ENABLE = "bullet.pubsub.pulsar.auth.enable";
    public static final String PULSAR_AUTH_PLUGIN_CLASS_NAME = "bullet.pubsub.pulsar.auth.plugin.class.name";
    public static final String PULSAR_AUTH_PARAMS_STRING = "bullet.pubsub.pulsar.auth.params.string";

    public static final String PULSAR_CLIENT_SERVICE_URL = "bullet.pubsub.pulsar.client.serviceUrl";
    public static final String PULSAR_CONSUMER_SUBSCRIPTION_NAME = "bullet.pubsub.pulsar.consumer.subscriptionName";
    public static final String PULSAR_CONSUMER_SUBSCRIPTION_TYPE = "bullet.pubsub.pulsar.consumer.subscriptionType";

    // PulsarConfig prefixes
    public static final String PULSAR_CLIENT_NAMESPACE = "bullet.pubsub.pulsar.client.";
    public static final String PULSAR_PRODUCER_NAMESPACE = "bullet.pubsub.pulsar.producer.";
    public static final String PULSAR_CONSUMER_NAMESPACE = "bullet.pubsub.pulsar.consumer.";

    // Defaults
    public static final String DEFAULT_PULSAR_CONFIGURATION = "bullet_pulsar_defaults.yaml";
    public static final boolean DEFAULT_PULSAR_AUTH_ENABLE = false;
    public static final int DEFAULT_PULSAR_MAX_UNCOMMITTED_MESSAGES = 50;
    public static final String DEFAULT_PULSAR_CONSUMER_SUBSCRIPTION_TYPE = SubscriptionType.Shared.toString();

    private static final String QUERY_SUBMISSION = PubSub.Context.QUERY_SUBMISSION.toString();
    private static final String QUERY_PROCESSING = PubSub.Context.QUERY_PROCESSING.toString();

    private static final Validator VALIDATOR = BulletConfig.getValidator();

    static {
        VALIDATOR.define(PUBSUB_CONTEXT_NAME);
        VALIDATOR.define(PULSAR_REQUEST_TOPIC_NAME);
        VALIDATOR.relate("If using QUERY_SUBMISSION, request topic name must be specified.", PUBSUB_CONTEXT_NAME, PULSAR_REQUEST_TOPIC_NAME)
                 .checkIf(isImpliedBy(isEqual(QUERY_SUBMISSION), Validator::isString))
                 .orFail();
        VALIDATOR.define(PULSAR_RESPONSE_TOPIC_NAME);
        VALIDATOR.relate("If using QUERY_SUBMISSION, response topic name must be specified.", PUBSUB_CONTEXT_NAME, PULSAR_RESPONSE_TOPIC_NAME)
                .checkIf(isImpliedBy(isEqual(QUERY_SUBMISSION), Validator::isString))
                .orFail();
        VALIDATOR.define(PULSAR_RESPONSE_TOPIC_NAMES);
        VALIDATOR.relate("If using QUERY_PROCESSING, response topic names must be specified.", PUBSUB_CONTEXT_NAME, PULSAR_RESPONSE_TOPIC_NAMES)
                 .checkIf(isImpliedBy(isEqual(QUERY_PROCESSING), Validator::isNonEmptyList))
                 .orFail();
        VALIDATOR.define(PULSAR_MAX_UNCOMMITTED_MESSAGES)
                 .checkIf(Validator::isPositiveInt)
                 .unless(isEqual(0))
                 .defaultTo(DEFAULT_PULSAR_MAX_UNCOMMITTED_MESSAGES);
        VALIDATOR.define(PULSAR_AUTH_ENABLE)
                 .checkIf(Validator::isBoolean)
                 .defaultTo(DEFAULT_PULSAR_AUTH_ENABLE);
        VALIDATOR.define(PULSAR_AUTH_PLUGIN_CLASS_NAME);
        VALIDATOR.relate("If Pulsar authentication is enabled, authentication class name must be specified.", PULSAR_AUTH_ENABLE, PULSAR_AUTH_PLUGIN_CLASS_NAME)
                 .checkIf(isImpliedBy(Validator::isTrue, Validator::isClassName))
                 .orFail();
        VALIDATOR.define(PULSAR_AUTH_PARAMS_STRING)
                 .checkIf(Validator::isString)
                 .unless(Validator::isNull)
                 .orFail();
        VALIDATOR.define(PULSAR_CLIENT_SERVICE_URL)
                 .checkIf(Validator::isString)
                 .orFail();
        VALIDATOR.define(PULSAR_CONSUMER_SUBSCRIPTION_NAME)
                 .checkIf(Validator::isString)
                 .orFail();
        VALIDATOR.define(PULSAR_CONSUMER_SUBSCRIPTION_TYPE)
                 .checkIf(Validator::isString)
                 .defaultTo(DEFAULT_PULSAR_CONSUMER_SUBSCRIPTION_TYPE);
    }

    /**
     * Creates a PulsarConfig with only default settings.
     */
    public PulsarConfig() {
        super(DEFAULT_PULSAR_CONFIGURATION);
        VALIDATOR.validate(this);
    }

    /**
     * Creates a PulsarConfig by reading in a file.
     *
     * @param file The file to read in to create the PulsarConfig.
     */
    public PulsarConfig(String file) {
        this(new Config(file));
    }

    /**
     * Creates a PulsarConfig from a Config.
     *
     * @param config The {@link Config} to copy settings from.
     */
    public PulsarConfig(Config config) {
        // Load default Pulsar settings. Merge additional settings in Config
        super(DEFAULT_PULSAR_CONFIGURATION);
        merge(config);
    }

    @Override
    public BulletConfig validate() {
        VALIDATOR.validate(this);
        return this;
    }
}
