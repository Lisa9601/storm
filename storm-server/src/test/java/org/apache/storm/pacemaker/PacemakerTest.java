package org.apache.storm.pacemaker;

import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.generated.HBMessage;
import org.apache.storm.generated.HBMessageData;
import org.apache.storm.generated.HBServerMessageType;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class PacemakerTest {

    private HBMessage message;
    private boolean auth;
    private HBServerMessageType expectedType;
    private boolean expected;


    public PacemakerTest(HBMessage message, boolean auth,HBServerMessageType expectedType, boolean expected) {
        this.message = message;
        this.auth = auth;
        this.expectedType = expectedType;
        this.expected = expected;
    }

    @Parameterized.Parameters
    public static Collection params() {

        HBMessageData validPath = new HBMessageData();
        validPath.set_path("path");

        return Arrays.asList(new Object[][] {
                { new HBMessage(HBServerMessageType.NOT_AUTHORIZED, null), false, null, false },
                { new HBMessage(HBServerMessageType.CREATE_PATH, validPath), false, HBServerMessageType.CREATE_PATH_RESPONSE, true}
        });
    }


    @Test
    public void handleMessage() {

        boolean result = false;

        Pacemaker pacemaker = new Pacemaker(null, new StormMetricsRegistry());

        HBMessage response = pacemaker.handleMessage(message, auth);

        if(response != null && expectedType == response.get_type() ){
            result = true;
        }

        Assert.assertEquals(expected, result);

    }
}