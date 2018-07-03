package com.tangzhe.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Created by 唐哲
 * 2018-02-12 16:34
 */
public class MyTopology {

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(MySpout.class.getSimpleName(), new MySpout());
        builder.setBolt(MyBolt.class.getSimpleName(), new MyBolt()).shuffleGrouping(MySpout.class.getSimpleName());

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(MyTopology.class.getSimpleName(), new Config(), builder.createTopology());
    }

}
