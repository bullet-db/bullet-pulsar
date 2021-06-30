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
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.ProducerCryptoFailureAction;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConfigurationDataUtils;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class PulsarPubSubTest {

    public static class MockAuthentication implements Authentication {

        public MockAuthentication() {
        }

        @Override
        public String getAuthMethodName() {
            return null;
        }

        @Override
        public AuthenticationDataProvider getAuthData() throws PulsarClientException {
            return null;
        }

        @Override
        public void configure(Map<String, String> map) {
        }

        @Override
        public void start() throws PulsarClientException {
        }

        @Override
        public void close() throws IOException {
        }
    }

    private PulsarConfig config;

    @BeforeMethod
    private void init() {
        config = new PulsarConfig();
    }

    @Test
    public void testDefaultConfiguration() throws PubSubException {
        config.set(BulletConfig.PUBSUB_CONTEXT_NAME, "QUERY_SUBMISSION");

        // Create PubSub from defaults
        PulsarPubSub pulsarPubSub = new PulsarPubSub(config);

        Map<String, Object> clientConf = config.getAllWithPrefix(Optional.empty(), PulsarConfig.PULSAR_CLIENT_NAMESPACE, true);

        // Check configuration maps are loaded and read correctly
        ClientConfigurationData clientConfData = ConfigurationDataUtils.loadData(clientConf, new ClientConfigurationData(), ClientConfigurationData.class);
        Assert.assertEquals(clientConfData.getServiceUrl(), "pulsar://localhost:6650");
        Assert.assertEquals(clientConfData.getOperationTimeoutMs(), 30000L);
        Assert.assertEquals(clientConfData.getStatsIntervalSeconds(), 60L);
        Assert.assertEquals(clientConfData.getNumIoThreads(), 1);
        Assert.assertEquals(clientConfData.getNumListenerThreads(), 1);
        Assert.assertEquals(clientConfData.getConnectionsPerBroker(), 1);
        Assert.assertEquals(clientConfData.isUseTcpNoDelay(), true);
        Assert.assertEquals(clientConfData.isUseTls(), false);
        Assert.assertEquals(clientConfData.getTlsTrustCertsFilePath(), "");
        Assert.assertEquals(clientConfData.isTlsAllowInsecureConnection(), false);
        Assert.assertEquals(clientConfData.isTlsHostnameVerificationEnable(), false);
        Assert.assertEquals(clientConfData.getConcurrentLookupRequest(), 5000);
        Assert.assertEquals(clientConfData.getMaxLookupRequest(), 50000);
        Assert.assertEquals(clientConfData.getMaxNumberOfRejectedRequestPerConnection(), 50);
        Assert.assertEquals(clientConfData.getKeepAliveIntervalSeconds(), 30);

        ProducerConfigurationData producerConfData = ConfigurationDataUtils.loadData(pulsarPubSub.getProducerConf(), new ProducerConfigurationData(), ProducerConfigurationData.class);
        Assert.assertEquals(producerConfData.getProducerName(), null);
        Assert.assertEquals(producerConfData.getSendTimeoutMs(), 30000L);
        Assert.assertEquals(producerConfData.isBlockIfQueueFull(), false);
        Assert.assertEquals(producerConfData.getMaxPendingMessages(), 1000);
        Assert.assertEquals(producerConfData.getMaxPendingMessagesAcrossPartitions(), 50000);
        Assert.assertEquals(producerConfData.getMessageRoutingMode(), MessageRoutingMode.RoundRobinPartition);
        Assert.assertEquals(producerConfData.getHashingScheme(), HashingScheme.JavaStringHash);
        Assert.assertEquals(producerConfData.getCryptoFailureAction(), ProducerCryptoFailureAction.FAIL);
        Assert.assertEquals(producerConfData.getBatchingMaxPublishDelayMicros(), 1000L);
        Assert.assertEquals(producerConfData.getBatchingMaxMessages(), 1000);
        Assert.assertEquals(producerConfData.isBatchingEnabled(), true);
        Assert.assertEquals(producerConfData.getCompressionType(), CompressionType.NONE);
        Assert.assertEquals(producerConfData.getInitialSequenceId(), null);

        ConsumerConfigurationData consumerConfData = ConfigurationDataUtils.loadData(pulsarPubSub.getConsumerConf(), new ConsumerConfigurationData(), ConsumerConfigurationData.class);
        Assert.assertEquals(consumerConfData.getSubscriptionName(), "mysubscription");
        Assert.assertEquals(consumerConfData.getSubscriptionType(), SubscriptionType.Shared);
        Assert.assertEquals(consumerConfData.getReceiverQueueSize(), 1000);
        Assert.assertEquals(consumerConfData.getAcknowledgementsGroupTimeMicros(), 100000L);
        Assert.assertEquals(consumerConfData.getMaxTotalReceiverQueueSizeAcrossPartitions(), 50000);
        Assert.assertEquals(consumerConfData.getConsumerName(), null);
        Assert.assertEquals(consumerConfData.getAckTimeoutMillis(), 0L);
        Assert.assertEquals(consumerConfData.getPriorityLevel(), 0);
        Assert.assertEquals(consumerConfData.getCryptoFailureAction(), ConsumerCryptoFailureAction.FAIL);
        Assert.assertEquals(consumerConfData.isReadCompacted(), false);
        Assert.assertEquals(consumerConfData.getSubscriptionInitialPosition(), SubscriptionInitialPosition.Latest);
        Assert.assertEquals(consumerConfData.getPatternAutoDiscoveryPeriod(), 1);
    }

    @Test
    public void testPulsarConfiguration() throws PubSubException {
        PulsarConfig config = new PulsarConfig("bullet_pulsar_not_defaults.yaml");
        config.set(BulletConfig.PUBSUB_CONTEXT_NAME, "QUERY_SUBMISSION");

        // Create PubSub from non-defaults config
        PulsarPubSub pulsarPubSub = new PulsarPubSub(config);

        Map<String, Object> clientConf = config.getAllWithPrefix(Optional.empty(), PulsarConfig.PULSAR_CLIENT_NAMESPACE, true);

        // Check configuration maps are loaded and read correctly
        ClientConfigurationData clientConfData = ConfigurationDataUtils.loadData(clientConf, new ClientConfigurationData(), ClientConfigurationData.class);
        Assert.assertEquals(clientConfData.getServiceUrl(), "pulsar://localhost:6650");
        Assert.assertEquals(clientConfData.getOperationTimeoutMs(), 300000L);
        Assert.assertEquals(clientConfData.getStatsIntervalSeconds(), 600L);
        Assert.assertEquals(clientConfData.getNumIoThreads(), 10);
        Assert.assertEquals(clientConfData.getNumListenerThreads(), 10);
        Assert.assertEquals(clientConfData.getConnectionsPerBroker(), 10);
        Assert.assertEquals(clientConfData.isUseTcpNoDelay(), false);
        Assert.assertEquals(clientConfData.isUseTls(), true);
        Assert.assertEquals(clientConfData.getTlsTrustCertsFilePath(), "filepath");
        Assert.assertEquals(clientConfData.isTlsAllowInsecureConnection(), true);
        Assert.assertEquals(clientConfData.isTlsHostnameVerificationEnable(), true);
        Assert.assertEquals(clientConfData.getConcurrentLookupRequest(), 50000);
        Assert.assertEquals(clientConfData.getMaxLookupRequest(), 500000);
        Assert.assertEquals(clientConfData.getMaxNumberOfRejectedRequestPerConnection(), 500);
        Assert.assertEquals(clientConfData.getKeepAliveIntervalSeconds(), 300);

        ProducerConfigurationData producerConfData = ConfigurationDataUtils.loadData(pulsarPubSub.getProducerConf(), new ProducerConfigurationData(), ProducerConfigurationData.class);
        Assert.assertEquals(producerConfData.getProducerName(), "producer");
        Assert.assertEquals(producerConfData.getSendTimeoutMs(), 300000L);
        Assert.assertEquals(producerConfData.isBlockIfQueueFull(), true);
        Assert.assertEquals(producerConfData.getMaxPendingMessages(), 10000);
        Assert.assertEquals(producerConfData.getMaxPendingMessagesAcrossPartitions(), 500000);
        Assert.assertEquals(producerConfData.getMessageRoutingMode(), MessageRoutingMode.SinglePartition);
        Assert.assertEquals(producerConfData.getHashingScheme(), HashingScheme.Murmur3_32Hash);
        Assert.assertEquals(producerConfData.getCryptoFailureAction(), ProducerCryptoFailureAction.SEND);
        Assert.assertEquals(producerConfData.getBatchingMaxPublishDelayMicros(), 10000L);
        Assert.assertEquals(producerConfData.getBatchingMaxMessages(), 10000);
        Assert.assertEquals(producerConfData.isBatchingEnabled(), false);
        Assert.assertEquals(producerConfData.getCompressionType(), CompressionType.LZ4);
        Assert.assertEquals(producerConfData.getInitialSequenceId(), Long.valueOf(5L));

        ConsumerConfigurationData consumerConfData = ConfigurationDataUtils.loadData(pulsarPubSub.getConsumerConf(), new ConsumerConfigurationData(), ConsumerConfigurationData.class);
        Assert.assertEquals(consumerConfData.getSubscriptionName(), "subscription");
        Assert.assertEquals(consumerConfData.getSubscriptionType(), SubscriptionType.Shared);
        Assert.assertEquals(consumerConfData.getReceiverQueueSize(), 10000);
        Assert.assertEquals(consumerConfData.getAcknowledgementsGroupTimeMicros(), 1000000L);
        Assert.assertEquals(consumerConfData.getMaxTotalReceiverQueueSizeAcrossPartitions(), 500000);
        Assert.assertEquals(consumerConfData.getConsumerName(), "consumer");
        Assert.assertEquals(consumerConfData.getAckTimeoutMillis(), 10L);
        Assert.assertEquals(consumerConfData.getPriorityLevel(), 10);
        Assert.assertEquals(consumerConfData.getCryptoFailureAction(), ConsumerCryptoFailureAction.DISCARD);
        Assert.assertEquals(consumerConfData.isReadCompacted(), true);
        Assert.assertEquals(consumerConfData.getSubscriptionInitialPosition(), SubscriptionInitialPosition.Earliest);
        Assert.assertEquals(consumerConfData.getPatternAutoDiscoveryPeriod(), 10);
    }

    @Test
    public void testSwitchContext() throws PubSubException {
        config.set(BulletConfig.PUBSUB_CONTEXT_NAME, "QUERY_SUBMISSION");

        // Create PubSub from defaults
        PulsarPubSub pulsarPubSub = new PulsarPubSub(config);

        Assert.assertEquals(pulsarPubSub.getContext(), PubSub.Context.QUERY_SUBMISSION);

        pulsarPubSub.switchContext(PubSub.Context.QUERY_PROCESSING, new PulsarConfig("bullet_pulsar_not_defaults.yaml"));
        Assert.assertEquals(pulsarPubSub.getContext(), PubSub.Context.QUERY_PROCESSING);
    }

    @Test
    public void testAuthentication() throws PubSubException {
        config.set(BulletConfig.PUBSUB_CONTEXT_NAME, "QUERY_SUBMISSION");
        config.set(PulsarConfig.PULSAR_AUTH_ENABLE, true);
        config.set(PulsarConfig.PULSAR_AUTH_PLUGIN_CLASS_NAME, "com.yahoo.bullet.pulsar.PulsarPubSubTest$MockAuthentication");
        config.set(PulsarConfig.PULSAR_AUTH_PARAMS_STRING, "");

        PulsarPubSub pulsarPubSub = new PulsarPubSub(config);

        SharedPulsarClient sharedPulsarClient = pulsarPubSub.getSharedPulsarClient();
        PulsarClient pulsarClient = sharedPulsarClient.getPulsarClient();

        Assert.assertTrue(pulsarClient instanceof PulsarClientImpl);
        Assert.assertTrue(((PulsarClientImpl) pulsarClient).getConfiguration().getAuthentication() instanceof MockAuthentication);
    }

    @Test
    public void testQuerySubmissionGetPublisher() throws Exception {
        config.set(BulletConfig.PUBSUB_CONTEXT_NAME, "QUERY_SUBMISSION");

        // Create PubSub from defaults
        PulsarPubSub pulsarPubSub = new PulsarPubSub(config);

        SharedPulsarClient sharedPulsarClient = Mockito.mock(SharedPulsarClient.class);
        PulsarClient pulsarClient = Mockito.mock(PulsarClient.class);
        ProducerBuilder<byte[]> producerBuilder = Mockito.mock(ProducerBuilder.class);
        Mockito.when(sharedPulsarClient.getPulsarClient()).thenReturn(pulsarClient);
        Mockito.when(pulsarClient.newProducer()).thenReturn(producerBuilder);
        Mockito.when(producerBuilder.loadConf(Mockito.any())).thenReturn(producerBuilder);
        Mockito.when(producerBuilder.topic(Mockito.any())).thenReturn(producerBuilder);
        Mockito.when(producerBuilder.create()).thenReturn(null);

        pulsarPubSub.setSharedPulsarClient(sharedPulsarClient);

        Assert.assertTrue(pulsarPubSub.getPublisher() instanceof PulsarQueryPublisher);

        List<Publisher> publishers = pulsarPubSub.getPublishers(10);
        Assert.assertEquals(publishers.size(), 10);
    }

    @Test
    public void testQuerySubmissionGetSubscriber() throws Exception {
        config.set(BulletConfig.PUBSUB_CONTEXT_NAME, "QUERY_SUBMISSION");

        // Create PubSub from defaults
        PulsarPubSub pulsarPubSub = new PulsarPubSub(config);

        SharedPulsarClient sharedPulsarClient = Mockito.mock(SharedPulsarClient.class);
        PulsarClient pulsarClient = Mockito.mock(PulsarClient.class);
        ConsumerBuilder<byte[]> consumerBuilder = Mockito.mock(ConsumerBuilder.class);
        Mockito.when(sharedPulsarClient.getPulsarClient()).thenReturn(pulsarClient);
        Mockito.when(pulsarClient.newConsumer()).thenReturn(consumerBuilder);
        Mockito.when(consumerBuilder.loadConf(Mockito.any())).thenReturn(consumerBuilder);
        Mockito.when(consumerBuilder.topics(Mockito.any())).thenReturn(consumerBuilder);
        Mockito.when(consumerBuilder.subscribe()).thenReturn(null);

        pulsarPubSub.setSharedPulsarClient(sharedPulsarClient);

        Assert.assertTrue(pulsarPubSub.getSubscriber() instanceof PulsarSubscriber);

        List<Subscriber> subscribers = pulsarPubSub.getSubscribers(10);
        Assert.assertEquals(subscribers.size(), 10);
    }

    @Test
    public void testQueryProcessingGetPublisher() throws PubSubException {
        config.set(BulletConfig.PUBSUB_CONTEXT_NAME, "QUERY_PROCESSING");

        // Create PubSub from defaults
        PulsarPubSub pulsarPubSub = new PulsarPubSub(config);

        Assert.assertTrue(pulsarPubSub.getPublisher() instanceof PulsarResponsePublisher);

        List<Publisher> publishers = pulsarPubSub.getPublishers(10);
        Assert.assertEquals(publishers.size(), 10);
    }

    @Test
    public void testQueryProcessingGetSubscriber() throws Exception {
        config.set(BulletConfig.PUBSUB_CONTEXT_NAME, "QUERY_PROCESSING");

        // Create PubSub from defaults
        PulsarPubSub pulsarPubSub = new PulsarPubSub(config);

        SharedPulsarClient sharedPulsarClient = Mockito.mock(SharedPulsarClient.class);
        PulsarClient pulsarClient = Mockito.mock(PulsarClient.class);
        ConsumerBuilder<byte[]> consumerBuilder = Mockito.mock(ConsumerBuilder.class);
        Mockito.when(sharedPulsarClient.getPulsarClient()).thenReturn(pulsarClient);
        Mockito.when(pulsarClient.newConsumer()).thenReturn(consumerBuilder);
        Mockito.when(consumerBuilder.loadConf(Mockito.any())).thenReturn(consumerBuilder);
        Mockito.when(consumerBuilder.topics(Mockito.any())).thenReturn(consumerBuilder);
        Mockito.when(consumerBuilder.subscribe()).thenReturn(null);

        pulsarPubSub.setSharedPulsarClient(sharedPulsarClient);

        Assert.assertTrue(pulsarPubSub.getSubscriber() instanceof PulsarSubscriber);

        List<Subscriber> subscribers = pulsarPubSub.getSubscribers(10);
        Assert.assertEquals(subscribers.size(), 10);
    }
}
