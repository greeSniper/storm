package com.gaiaworks.storm.sum;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Created by 唐哲
 * 2018-02-06 16:30
 */
public class Main {

    public static void main(String[] args) {
        //TopologyBuilder根据Spout和Bolt来构建出Topology
        //Storm中任何一个作业都是通过Topology的方式进行提交的
        //Topology中需要指定Spout和Bolt的执行顺序
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("DataSourceSpout", new DataSourceSpout());
        builder.setBolt("SumBolt", new SumBolt()).shuffleGrouping("DataSourceSpout");

        //创建一个本地Storm集群：本地模式运行，不需要搭建Storm集群
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("LocalSumStormAckerTopology", new Config(), builder.createTopology());
    }

}
