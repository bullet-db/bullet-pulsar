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
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PulsarQueryPublisherTest {

    private SharedPulsarClient sharedPulsarClient;
    private PulsarClient pulsarClient;
    private ProducerBuilder<byte[]> producerBuilder;
    private Producer<byte[]> producer;

    @BeforeMethod
    public void setup() throws Exception {
        sharedPulsarClient = Mockito.mock(SharedPulsarClient.class);
        pulsarClient = Mockito.mock(PulsarClient.class);
        producerBuilder = Mockito.mock(ProducerBuilder.class);
        producer = Mockito.mock(Producer.class);
        Mockito.when(sharedPulsarClient.getPulsarClient()).thenReturn(pulsarClient);
        Mockito.when(pulsarClient.newProducer()).thenReturn(producerBuilder);
        Mockito.when(producerBuilder.loadConf(Mockito.any())).thenReturn(producerBuilder);
        Mockito.when(producerBuilder.topic(Mockito.any())).thenReturn(producerBuilder);
        Mockito.when(producerBuilder.create()).thenReturn(producer);
    }

    @Test
    public void testSend() throws Exception {
        ArgumentCaptor<byte[]> arg = ArgumentCaptor.forClass(byte[].class);
        Mockito.doReturn(null).when(producer).sendAsync(arg.capture());

        PulsarQueryPublisher publisher = new PulsarQueryPublisher(sharedPulsarClient, null, "queryTopicName", "responseTopicName");
        publisher.send("id", "hello world");

        PubSubMessage pubSubMessage = SerializerDeserializer.fromBytes(arg.getValue());
        Assert.assertEquals(pubSubMessage.getId(), "id");
        Assert.assertEquals(pubSubMessage.getContent(), "hello world");
        PulsarMetadata metadata = (PulsarMetadata) pubSubMessage.getMetadata();
        Assert.assertEquals(metadata.getTopicName(), "responseTopicName");

        // coverage
        publisher.send(new PubSubMessage("id", "hello world!", new Metadata()));

        pubSubMessage = SerializerDeserializer.fromBytes(arg.getValue());
        Assert.assertEquals(pubSubMessage.getId(), "id");
        Assert.assertEquals(pubSubMessage.getContent(), "hello world!");
        metadata = (PulsarMetadata) pubSubMessage.getMetadata();
        Assert.assertEquals(metadata.getTopicName(), "responseTopicName");

        Assert.assertNotNull(publisher.getSharedPulsarClient());

        publisher.close();
        Assert.assertNull(publisher.getSharedPulsarClient());

        publisher.close(); // coverage; does nothing
        Assert.assertNull(publisher.getSharedPulsarClient());
    }

    @Test(expectedExceptions = PubSubException.class, expectedExceptionsMessageRegExp = "Could not create producer\\.")
    public void testSendThrows() throws Exception {
        Mockito.doThrow(new PulsarClientException("mock exception")).when(producerBuilder).create();

        Publisher publisher = new PulsarQueryPublisher(sharedPulsarClient, null, "queryTopicName", "responseTopicName");
        publisher.send("id", "hello world");
    }
}
