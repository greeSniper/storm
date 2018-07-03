package com.gaiaworks.service;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Created by 唐哲
 * 2018-02-13 10:29
 */
public class WordCountTopology {

    public static void main(String[] args) {
        //通过TopologyBuilder根据Spout和Bolt构建Topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(DataSourceSpout.class.getSimpleName(), new DataSourceSpout());
        builder.setBolt(SplitBolt.class.getSimpleName(), new SplitBolt()).shuffleGrouping(DataSourceSpout.class.getSimpleName());
        builder.setBolt(CountBolt.class.getSimpleName(), new CountBolt()).shuffleGrouping(SplitBolt.class.getSimpleName());

        //创建本地集群
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(WordCountTopology.class.getSimpleName(), new Config(), builder.createTopology());
    }

}
