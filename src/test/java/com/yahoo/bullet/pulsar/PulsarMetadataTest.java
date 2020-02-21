/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pulsar;

import com.yahoo.bullet.pubsub.Metadata;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashMap;

public class PulsarMetadataTest {
    @Test
    public void testCreationWithoutMetadata() {
        PulsarMetadata metadata = new PulsarMetadata("foo");
        Assert.assertNull(metadata.getContent());
        Assert.assertNull(metadata.getSignal());
        Assert.assertEquals(metadata.getTopicName(), "foo");
    }

    @Test
    public void testCreationWithMetadata() {
        PulsarMetadata metadata = new PulsarMetadata(new Metadata(Metadata.Signal.CUSTOM, new HashMap<>()), "foo");
        Assert.assertEquals(metadata.getSignal(), Metadata.Signal.CUSTOM);
        Assert.assertEquals(metadata.getContent(), Collections.emptyMap());
        Assert.assertEquals(metadata.getTopicName(), "foo");
    }
}
