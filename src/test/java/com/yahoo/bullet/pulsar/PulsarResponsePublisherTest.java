/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pulsar;

import com.yahoo.bullet.common.SerializerDeserializer;
import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.PubSubMessage;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PulsarResponsePublisherTest {

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
        Mockito.when(producerBuilder.clone()).thenReturn(producerBuilder);
    }

    @Test
    public void testSend() throws Exception {
        ArgumentCaptor<String> arg1 = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<byte[]> arg2 = ArgumentCaptor.forClass(byte[].class);
        Mockito.doReturn(producerBuilder).when(producerBuilder).topic(arg1.capture());
        Mockito.doReturn(producer).when(producerBuilder).create();
        Mockito.doReturn(null).when(producer).sendAsync(arg2.capture());

        PulsarResponsePublisher publisher = new PulsarResponsePublisher(sharedPulsarClient, null);

        // first message
        PubSubMessage messageToSend = new PubSubMessage("id", "hello world", new PulsarMetadata("responseTopicName"));
        publisher.send(messageToSend);
        Assert.assertEquals(arg1.getValue(), "responseTopicName");

        PubSubMessage pubSubMessage = SerializerDeserializer.fromBytes(arg2.getValue());
        Assert.assertEquals(pubSubMessage.getId(), "id");
        Assert.assertEquals(pubSubMessage.getContent(), "hello world");
        Assert.assertEquals(((PulsarMetadata) pubSubMessage.getMetadata()).getTopicName(), "responseTopicName");

        // second message with different response topic
        messageToSend = new PubSubMessage("id 2", "hello world 2", new PulsarMetadata("responseTopicName2"));
        publisher.send(messageToSend);
        Assert.assertEquals(arg1.getValue(), "responseTopicName2");

        pubSubMessage = SerializerDeserializer.fromBytes(arg2.getValue());
        Assert.assertEquals(pubSubMessage.getId(), "id 2");
        Assert.assertEquals(pubSubMessage.getContent(), "hello world 2");
        Assert.assertEquals(((PulsarMetadata) pubSubMessage.getMetadata()).getTopicName(), "responseTopicName2");

        // third message with same topic
        messageToSend = new PubSubMessage("id 3", "hello world 3", new PulsarMetadata("responseTopicName2"));
        publisher.send(messageToSend);
        Assert.assertEquals(arg1.getValue(), "responseTopicName2");

        pubSubMessage = SerializerDeserializer.fromBytes(arg2.getValue());
        Assert.assertEquals(pubSubMessage.getId(), "id 3");
        Assert.assertEquals(pubSubMessage.getContent(), "hello world 3");
        Assert.assertEquals(((PulsarMetadata) pubSubMessage.getMetadata()).getTopicName(), "responseTopicName2");

        // should only have 2 producers
        Assert.assertEquals(publisher.getProducers().size(), 2);

        Assert.assertNotNull(publisher.getSharedPulsarClient());

        publisher.close();
        Assert.assertNull(publisher.getSharedPulsarClient());

        publisher.close(); // coverage; does nothing
        Assert.assertNull(publisher.getSharedPulsarClient());
    }

    @Test(expectedExceptions = PubSubException.class, expectedExceptionsMessageRegExp = "Could not create producer\\.")
    public void testSendThrows() throws Exception {
        Mockito.doReturn(producerBuilder).when(producerBuilder).topic(Mockito.anyString());
        Mockito.doThrow(new PulsarClientException("mock exception")).when(producerBuilder).create();

        PulsarResponsePublisher publisher = new PulsarResponsePublisher(sharedPulsarClient, null);

        PubSubMessage messageToSend = new PubSubMessage("id", "hello world", new PulsarMetadata("responseTopicName"));
        publisher.send(messageToSend);
    }
}
