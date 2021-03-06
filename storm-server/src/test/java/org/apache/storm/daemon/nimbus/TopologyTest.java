package org.apache.storm.daemon.nimbus;

import org.apache.commons.io.FileUtils;
import org.apache.storm.Config;
import org.apache.storm.blobstore.BlobStore;
import org.apache.storm.blobstore.LocalFsBlobStore;
import org.apache.storm.generated.*;
import org.apache.storm.nimbus.NimbusInfo;
import org.apache.storm.security.auth.SingleUserPrincipal;
import org.apache.storm.testing.InProcessZookeeper;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.ConfigUtils;
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
public class TopologyTest {

    private InProcessZookeeper zk;
    private File baseFile;
    private TopoCache cache;

    private String topoName;
    private Subject subject;
    private boolean expected;

    public TopologyTest(String topoName, Subject subject, boolean expected) {
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

        SettableBlobMeta meta = new SettableBlobMeta();
        AccessControl acc = new AccessControl(AccessControlType.OTHER, 0x00);
        acc.set_name("invalid_user");
        meta.add_to_acl(acc);
        Subject subject = new Subject();
        SingleUserPrincipal user = new SingleUserPrincipal("valid_user");
        subject.getPrincipals().add(user);

        store.createBlob(ConfigUtils.masterStormCodeKey("topology2"),Utils.serialize(topology), meta, subject);

    }

    @After
    public void tearDown() throws Exception {
        cache.clear();
        FileUtils.deleteDirectory(baseFile);
        zk.close();
    }


    @Parameterized.Parameters
    public static Collection params() {

        Subject subject1 = new Subject();
        SingleUserPrincipal valid_user = new SingleUserPrincipal("valid_user");
        subject1.getPrincipals().add(valid_user);

        Subject subject2 = new Subject();
        SingleUserPrincipal invalid_user = new SingleUserPrincipal("invalid_user");
        subject2.getPrincipals().add(invalid_user);

        return Arrays.asList(new Object[][] {
                { "topology1", null, true },
                { "topology2", subject1, true },
                { "topology2", subject2, false},    //AuthorizationException
                { "topology3", null, false },       //KeyNotFoundException
        });
    }


    @Test
    public void readTest(){

        boolean result = true;

        try {
            cache.readTopology(topoName,subject);

        } catch (AuthorizationException | KeyNotFoundException | NullPointerException | IOException e) {
            e.printStackTrace();
            result = false;
        }

        Assert.assertEquals(expected,result);
    }


    @Test
    public void deleteTest(){

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