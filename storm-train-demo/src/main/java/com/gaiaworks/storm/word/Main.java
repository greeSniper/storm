package com.gaiaworks.storm.word;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Created by 唐哲
 * 2018-02-06 17:20
 */
public class Main {

    public static void main(String[] args) {
        //通过TopologyBuilder根据Spout和Bolt构建Topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("dataSourceSpout", new DataSourceSpout());
        builder.setBolt("splitBolt", new SplitBolt()).shuffleGrouping("dataSourceSpout");
        builder.setBolt("countBolt", new CountBolt()).shuffleGrouping("splitBolt");

        //创建本地集群
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Main", new Config(), builder.createTopology());
    }

}
