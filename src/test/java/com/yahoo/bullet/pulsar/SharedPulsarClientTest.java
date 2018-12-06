/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pulsar;

import com.yahoo.bullet.pubsub.PubSubException;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class SharedPulsarClientTest {

    @Test
    public void testSharedPulsarClientGetAndClose() throws Exception {
        PulsarClient pulsarClient = Mockito.mock(PulsarClient.class);
        ClientBuilder clientBuilder = Mockito.mock(ClientBuilder.class);
        Mockito.when(clientBuilder.clone()).thenReturn(clientBuilder);
        Mockito.when(clientBuilder.build()).thenReturn(pulsarClient);

        SharedPulsarClient sharedPulsarClient = new SharedPulsarClient(clientBuilder);

        AtomicInteger atom = sharedPulsarClient.getRefCount();
        Assert.assertEquals(atom.get(), 0);

        sharedPulsarClient.getPulsarClient();
        sharedPulsarClient.getPulsarClient();
        Assert.assertEquals(atom.get(), 2);

        sharedPulsarClient.close();
        Assert.assertEquals(atom.get(), 1);

        sharedPulsarClient.close();
        Assert.assertEquals(atom.get(), 0);

        sharedPulsarClient.close();
        Assert.assertEquals(atom.get(), 0);
    }

    @Test(expectedExceptions = PubSubException.class, expectedExceptionsMessageRegExp = "Could not create PulsarClient\\.")
    public void testGetPulsarClientThrows() throws Exception {
        ClientBuilder clientBuilder = Mockito.mock(ClientBuilder.class);
        Mockito.when(clientBuilder.clone()).thenReturn(clientBuilder);
        Mockito.when(clientBuilder.build()).thenThrow(new PulsarClientException("mock exception"));

        new SharedPulsarClient(clientBuilder).getPulsarClient();
    }
}
