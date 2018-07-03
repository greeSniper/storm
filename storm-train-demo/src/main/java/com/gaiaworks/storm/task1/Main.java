package com.gaiaworks.storm.task1;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

import java.util.Random;

/**
 * Created by 唐哲
 * 2018-02-06 17:48
 */
public class Main {

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("mySpout", new MySpout());
        builder.setBolt("myBoltOne", new MyBoltOne()).shuffleGrouping("mySpout");

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Main", new Config(), builder.createTopology());
    }

}
