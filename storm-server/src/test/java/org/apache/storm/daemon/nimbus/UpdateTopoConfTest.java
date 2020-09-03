package org.apache.storm.daemon.nimbus;

import org.apache.commons.io.FileUtils;
import org.apache.storm.Config;
import org.apache.storm.blobstore.BlobStore;
import org.apache.storm.blobstore.LocalFsBlobStore;
import org.apache.storm.generated.*;
import org.apache.storm.nimbus.NimbusInfo;
import org.apache.storm.security.auth.SingleUserPrincipal;
import org.apache.storm.testing.InProcessZookeeper;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.Utils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.security.auth.Subject;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;


@RunWith(Parameterized.class)
public class UpdateTopoConfTest {

    private static InProcessZookeeper zk;
    private static File baseFile;
    private static TopoCache cache;

    private String topoName;
    private Subject subject;
    private Map<String,Object> conf;
    private boolean expected;

    public UpdateTopoConfTest(String topoName, Subject subject, Map<String,Object> conf, boolean expected) {
        this.topoName = topoName;
        this.subject = subject;
        this.conf = conf;
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

        Map<String, Object> topoConf = new Config();

        cache.addTopoConf("topology1", new Subject(), topoConf);

        SettableBlobMeta meta = new SettableBlobMeta();
        AccessControl acc = new AccessControl(AccessControlType.OTHER, 0x00);
        acc.set_name("invalid_user");
        meta.add_to_acl(acc);
        Subject subject = new Subject();
        SingleUserPrincipal user = new SingleUserPrincipal("valid_user");
        subject.getPrincipals().add(user);

        store.createBlob(ConfigUtils.masterStormConfKey("topology2"),Utils.toCompressedJsonConf(topoConf), meta, subject);

    }

    @AfterClass
    public static void tearDown() throws Exception {
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

        Map<String, Object> conf = new Config();
        conf.put("prova","prova");

        return Arrays.asList(new Object[][] {
                { "topology1", null, conf, true },
                { "topology1", null, null, false },     //NullPointerException
                { "topology2", subject1, conf, true },
                { "topology2", subject2, conf, false},  //AuthorizationException
                { "topology3", null, conf, false },     //KeyNotFoundException
        });
    }


    @Test
    public void test(){

        boolean result = true;

        try {

            cache.updateTopoConf(topoName,subject,conf);

            Map<String, Object> topoConf = cache.readTopoConf(topoName,subject);

            if(!conf.equals(topoConf)){
                result = false;
            }

        } catch (AuthorizationException | KeyNotFoundException | NullPointerException | IOException e) {
            e.printStackTrace();
            result = false;
        }

        Assert.assertEquals(expected,result);
    }

}