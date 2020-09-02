package org.apache.storm.daemon.nimbus;

import org.apache.commons.io.FileUtils;
import org.apache.storm.Config;
import org.apache.storm.blobstore.BlobStore;
import org.apache.storm.blobstore.LocalFsBlobStore;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.KeyAlreadyExistsException;
import org.apache.storm.generated.KeyNotFoundException;
import org.apache.storm.generated.StormTopology;
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
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;


@RunWith(Parameterized.class)
public class DeleteTopologyTest {

    private InProcessZookeeper zk;
    private File baseFile;
    private TopoCache cache;

    private String topoName;
    private Subject subject;
    private boolean expected;

    public DeleteTopologyTest(String topoName, Subject subject, boolean expected) {
        this.topoName = topoName;
        this.subject = subject;
        this.expected = expected;
    }

    @Before
    public void setUp() throws Exception {
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


        TopologyBuilder builder = new TopologyBuilder();
        StormTopology  topology = builder.createTopology();

        cache.addTopology("topology1", new Subject(), topology);

    }

    @After
    public void tearDown() throws Exception {
        FileUtils.deleteDirectory(baseFile);
        zk.close();
    }


    @Parameterized.Parameters
    public static Collection params() {

        return Arrays.asList(new Object[][] {
                { "topology1", new Subject(), true },
                { "topology2", null, false },
                //{ "topology1", null , false }
        });
    }


    @Test
    public void test(){

        boolean result = true;

        try {
            cache.deleteTopology(topoName,subject);
        } catch (AuthorizationException | KeyNotFoundException | NullPointerException e) {
            e.printStackTrace();
            result = false;
        }

        Assert.assertEquals(expected,result);
    }



}