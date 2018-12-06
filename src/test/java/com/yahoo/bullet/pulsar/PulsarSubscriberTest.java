/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pulsar;

import com.yahoo.bullet.common.SerializerDeserializer;
import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.PubSubMessage;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.shade.io.netty.buffer.Unpooled;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;

public class PulsarSubscriberTest {

    private SharedPulsarClient sharedPulsarClient;
    private PulsarClient pulsarClient;
    private ConsumerBuilder<byte[]> consumerBuilder;
    private Consumer<byte[]> consumer;

    @BeforeMethod
    public void setup() throws Exception {
        sharedPulsarClient = Mockito.mock(SharedPulsarClient.class);
        pulsarClient = Mockito.mock(PulsarClient.class);
        consumerBuilder = Mockito.mock(ConsumerBuilder.class);
        consumer = Mockito.mock(Consumer.class);
        Mockito.when(sharedPulsarClient.getPulsarClient()).thenReturn(pulsarClient);
        Mockito.when(pulsarClient.newConsumer()).thenReturn(consumerBuilder);
        Mockito.when(consumerBuilder.loadConf(Mockito.any())).thenReturn(consumerBuilder);
        Mockito.when(consumerBuilder.topics(Mockito.any())).thenReturn(consumerBuilder);
        Mockito.when(consumerBuilder.subscribe()).thenReturn(consumer);
    }

    @Test
    public void testGetMessages() throws Exception {
        PulsarSubscriber subscriber = new PulsarSubscriber(sharedPulsarClient, null, null, 50);
        PubSubMessage pubSubMessage1 = new PubSubMessage("id", "hello world");
        PubSubMessage pubSubMessage2 = new PubSubMessage("id", "hello world!");

        Mockito.when(consumer.receive(Mockito.anyInt(), Mockito.any()))
               .thenReturn(new MessageImpl<>("mytopic", "1:1", Collections.emptyMap(), Unpooled.wrappedBuffer(SerializerDeserializer.toBytes(pubSubMessage1)), null))
               .thenReturn(new MessageImpl<>("mytopic", "2:2", Collections.emptyMap(), Unpooled.wrappedBuffer(SerializerDeserializer.toBytes(null)), null))
               .thenReturn(new MessageImpl<>("mytopic", "3:3", Collections.emptyMap(), Unpooled.wrappedBuffer(SerializerDeserializer.toBytes(pubSubMessage2)), null))
               .thenReturn(null);

        List<PubSubMessage> messages = subscriber.getMessages();
        Assert.assertEquals(messages.size(), 3);
        Assert.assertEquals(messages.get(0).getContent(), "hello world");
        Assert.assertEquals(messages.get(1), null);
        Assert.assertEquals(messages.get(2).getContent(), "hello world!");

        Assert.assertNotNull(subscriber.getSharedPulsarClient());

        subscriber.close();
        Assert.assertNull(subscriber.getSharedPulsarClient());

        subscriber.close(); // coverage; does nothing
        Assert.assertNull(subscriber.getSharedPulsarClient());
    }

    @Test(expectedExceptions = PubSubException.class, expectedExceptionsMessageRegExp = "Could not create consumer\\.")
    public void testConstructorThrows() throws Exception {
        Mockito.doThrow(new PulsarClientException("mock exception")).when(consumerBuilder).subscribe();

        new PulsarSubscriber(sharedPulsarClient, null, null, 50);
    }

    @Test(expectedExceptions = PubSubException.class, expectedExceptionsMessageRegExp = "Could not read from consumer\\.")
    public void testGetMessagesThrows() throws Exception {
        Mockito.doThrow(new PulsarClientException("mock exception")).when(consumer).receive(Mockito.anyInt(), Mockito.any());

        PulsarSubscriber subscriber = new PulsarSubscriber(sharedPulsarClient, null, null, 50);
        subscriber.getMessages();
    }
}
