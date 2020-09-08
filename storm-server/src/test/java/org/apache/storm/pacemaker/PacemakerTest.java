package org.apache.storm.pacemaker;

import org.mockito.Mock;
import org.powermock.reflect.Whitebox;
import org.apache.storm.generated.HBPulse;
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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


@RunWith(Parameterized.class)
public class PacemakerTest {

    private HBMessage message;
    private boolean auth;
    private HBServerMessageType expectedType;
    private boolean expected;
    @Mock
    Map<String, byte[]> heartbeats;


    public PacemakerTest(HBMessage message, boolean auth, HBServerMessageType expectedType, boolean expected) {
        this.message = message;
        this.auth = auth;
        this.expectedType = expectedType;
        this.expected = expected;

        this.heartbeats = new ConcurrentHashMap<>();
        heartbeats.put("path/subpath","details".getBytes());
        heartbeats.put("path", "details".getBytes());
    }

    @Parameterized.Parameters
    public static Collection params() {

        HBMessageData validPath = new HBMessageData();
        validPath.set_path("path");

        HBMessageData pulseData = new HBMessageData();
        HBPulse pulse = new HBPulse();
        pulse.set_id("path2");
        pulse.set_details("details".getBytes());
        pulseData.set_pulse(pulse);

        return Arrays.asList(new Object[][] {
                { null, false, null, false },
                { new HBMessage(HBServerMessageType.NOT_AUTHORIZED, null), false, null, false },
                { new HBMessage(HBServerMessageType.CREATE_PATH, validPath), false, HBServerMessageType.CREATE_PATH_RESPONSE, true},
                { new HBMessage(HBServerMessageType.EXISTS, validPath), false, HBServerMessageType.NOT_AUTHORIZED, true},
                { new HBMessage(HBServerMessageType.EXISTS, validPath), true, HBServerMessageType.EXISTS_RESPONSE, true},
                { new HBMessage(HBServerMessageType.SEND_PULSE, pulseData), false, HBServerMessageType.SEND_PULSE_RESPONSE, true},
                { new HBMessage(HBServerMessageType.GET_ALL_PULSE_FOR_PATH, validPath), true, HBServerMessageType.GET_ALL_PULSE_FOR_PATH_RESPONSE, true},
                { new HBMessage(HBServerMessageType.GET_ALL_PULSE_FOR_PATH, validPath), false, HBServerMessageType.NOT_AUTHORIZED, true},
                { new HBMessage(HBServerMessageType.GET_ALL_NODES_FOR_PATH, validPath), true, HBServerMessageType.GET_ALL_NODES_FOR_PATH_RESPONSE, true},
                { new HBMessage(HBServerMessageType.GET_ALL_NODES_FOR_PATH, validPath), false, HBServerMessageType.NOT_AUTHORIZED, true},
                { new HBMessage(HBServerMessageType.GET_PULSE, validPath), true, HBServerMessageType.GET_PULSE_RESPONSE, true},
                { new HBMessage(HBServerMessageType.GET_PULSE, validPath), false, HBServerMessageType.NOT_AUTHORIZED, true},
                { new HBMessage(HBServerMessageType.DELETE_PATH, validPath), true, HBServerMessageType.DELETE_PATH_RESPONSE, true},
                { new HBMessage(HBServerMessageType.DELETE_PULSE_ID, validPath), false, HBServerMessageType.DELETE_PULSE_ID_RESPONSE, true}

        });
    }


    @Test
    public void handleMessageTest() {

        boolean result = false;

        Pacemaker pacemaker = new Pacemaker(null, new StormMetricsRegistry());

        Whitebox.setInternalState(pacemaker,"heartbeats",heartbeats);

        HBMessage response = null;

        try{
            response = pacemaker.handleMessage(message, auth);

        } catch( NullPointerException e ){
            e.printStackTrace();
        }

        if(response != null && expectedType == response.get_type() ){
            result = true;
        }

        Assert.assertEquals(expected, result);

    }
}