package org.apache.storm.daemon.nimbus;

import org.apache.commons.io.FileUtils;
import org.apache.storm.Config;
import org.apache.storm.blobstore.*;
import org.apache.storm.generated.*;
import org.apache.storm.nimbus.NimbusInfo;
import org.apache.storm.testing.InProcessZookeeper;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.security.auth.Subject;

import java.io.File;
import java.io.IOException;
import java.security.AccessControlContext;
import java.security.ProtectionDomain;
import java.util.*;


@RunWith(Parameterized.class)
public class AddTopologyTest {

    private static InProcessZookeeper zk;
    private static File baseFile;
    private static TopoCache cache;

    private String topoName;
    private Subject subject;
    private StormTopology topology;
    private boolean expected;

    public AddTopologyTest(String topoName, Subject subject, StormTopology topology, boolean expected) {
        this.topoName = topoName;
        this.subject = subject;
        this.topology = topology;
        this.expected = expected;
    }

    @BeforeClass
    public static void setUp() throws Exception {
        baseFile = new File("target/blob-store-test");

        try {
            zk = new InProcessZookeeper();
        } catch (Exception e) {
            e.printStackTrace();
        }

        Map<String, Object> conf = Utils.readStormConfig();
        conf.put(Config.STORM_ZOOKEEPER_PORT, zk.getPort());
        conf.put(Config.STORM_LOCAL_DIR, baseFile.getAbsolutePath());
        conf.put(Config.STORM_PRINCIPAL_TO_LOCAL_PLUGIN, "org.apache.storm.security.auth.DefaultPrincipalToLocal");

        NimbusInfo nimbusInfo = new NimbusInfo("localhost", 0, false);

        BlobStore store = new LocalFsBlobStore();
        store.prepare(conf,null,nimbusInfo,null);


        Config theconf = new Config();
        theconf.putAll(Utils.readStormConfig());

        cache = new TopoCache(store, theconf);

    }

    @AfterClass
    public static void tearDown() throws Exception {
        FileUtils.deleteDirectory(baseFile);
        zk.close();
    }


    @Parameterized.Parameters
    public static Collection params() {

        TopologyBuilder builder = new TopologyBuilder();
        StormTopology  topology = builder.createTopology();


        return Arrays.asList(new Object[][] {
                { "topology1", new Subject(), topology, true },
                { "topology1", null, topology, false },
                { "topology2", new Subject(), null, false },
        });
    }


    @Test
    public void test(){

        boolean result = true;

        try {
            cache.addTopology(topoName, subject, topology);
        } catch (AuthorizationException | KeyAlreadyExistsException | IOException | NullPointerException e) {
            e.printStackTrace();
            result = false;
        }

        Assert.assertEquals(expected,result);
    }



}