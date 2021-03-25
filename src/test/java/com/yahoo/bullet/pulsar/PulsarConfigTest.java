/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pulsar;

import com.yahoo.bullet.common.BulletConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

public class PulsarConfigTest {
    @Test
    public void testDefaults() {
        PulsarConfig pulsarConfig = new PulsarConfig();
        Assert.assertEquals(((Number) pulsarConfig.get(PulsarConfig.PULSAR_MAX_UNCOMMITTED_MESSAGES)).intValue(), 50);
        Assert.assertEquals(pulsarConfig.get(PulsarConfig.PULSAR_AUTH_ENABLE), false);
        Assert.assertNull(pulsarConfig.get("dne"));
    }

    @Test
    public void testLoadFromFile() {
        PulsarConfig pulsarConfig = new PulsarConfig("test_config.yaml");
        Assert.assertEquals(((Number) pulsarConfig.get(PulsarConfig.PULSAR_MAX_UNCOMMITTED_MESSAGES)).intValue(), 100);
        Assert.assertNull(pulsarConfig.get("dne"));
    }

    @Test
    public void testCopyConfig() {
        BulletConfig config = new BulletConfig();
        config.set(PulsarConfig.PULSAR_MAX_UNCOMMITTED_MESSAGES, 100L);
        Assert.assertEquals(((Number) config.get(PulsarConfig.PULSAR_MAX_UNCOMMITTED_MESSAGES)).intValue(), 100);
        Assert.assertNull(config.get(PulsarConfig.PULSAR_AUTH_ENABLE));
        Assert.assertNull(config.get("dne"));

        PulsarConfig pulsarConfig = new PulsarConfig(config);
        Assert.assertEquals(((Number) config.get(PulsarConfig.PULSAR_MAX_UNCOMMITTED_MESSAGES)).intValue(), 100);
        Assert.assertEquals(pulsarConfig.get(PulsarConfig.PULSAR_AUTH_ENABLE), false);
        Assert.assertNull(pulsarConfig.get("dne"));
    }
}

