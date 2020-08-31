package org.apache.storm.daemon.nimbus;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.blobstore.*;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.KeyAlreadyExistsException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import org.junit.Test;

import javax.security.auth.Subject;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import static org.junit.Assert.*;

public class TopoCacheTest {

    @Test
    public void addTopologyTest(){

        Config theconf = new Config();
        //theconf.putAll(Utils.readStormConfig());

        BlobStore store = new LocalFsBlobStore();

        TopoCache cache = new TopoCache(store, theconf);

        try {
            Enumeration<URL> resources = Thread.currentThread().getContextClassLoader().getResources("defaults.yaml");
            List<URL> ret = new ArrayList<URL>();
            while (resources.hasMoreElements()) {
                ret.add(resources.nextElement());
            }

            System.out.println(ret);

        } catch (IOException e) {
            e.printStackTrace();
        }


        TopologyBuilder builder = new TopologyBuilder();

        StormTopology  topology = builder.createTopology();

        try {
            cache.addTopology("toplogy1", new Subject(), topology);
        } catch (AuthorizationException | KeyAlreadyExistsException | IOException e) {
            e.printStackTrace();
        }


    }



}