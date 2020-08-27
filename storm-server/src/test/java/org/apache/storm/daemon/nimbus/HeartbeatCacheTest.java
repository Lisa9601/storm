package org.apache.storm.daemon.nimbus;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class HeartbeatCacheTest {

    @Test
    public void test(){

        HeartbeatCache cache = new HeartbeatCache();

        cache.addEmptyTopoForTests("prova");

        cache.removeTopo("prova");

        Assert.assertTrue(true);
    }


}