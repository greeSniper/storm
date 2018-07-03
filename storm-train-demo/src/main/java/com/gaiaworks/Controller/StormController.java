package com.gaiaworks.Controller;

import com.gaiaworks.storm.sum.DataSourceSpout;
import com.gaiaworks.storm.sum.SumBolt;
import com.gaiaworks.storm.task1.MyBoltOne;
import com.gaiaworks.storm.task1.MySpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Created by 唐哲
 * 2018-02-06 16:32
 */
@RestController
public class StormController {

    @Autowired
    private DataSourceSpout dataSourceSpout;
    @Autowired
    private SumBolt sumBolt;
    @Autowired
    private MySpout mySpout;
    @Autowired
    private MyBoltOne myBoltOne;

    @GetMapping("/sum")
    public String sum() {
        //TopologyBuilder根据Spout和Bolt来构建出Topology
        //Storm中任何一个作业都是通过Topology的方式进行提交的
        //Topology中需要指定Spout和Bolt的执行顺序
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("dataSourceSpout", dataSourceSpout);
        builder.setBolt("sumBolt", sumBolt).shuffleGrouping("dataSourceSpout");

        //创建一个本地Storm集群：本地模式运行，不需要搭建Storm集群
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("StormTest", new Config(), builder.createTopology());

        return "storm sum test";
    }

    @GetMapping("/task1")
    public String task1() {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("mySpout", mySpout);
        builder.setBolt("myBoltOne", myBoltOne).shuffleGrouping("mySpout");

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("StormTest", new Config(), builder.createTopology());

        return "task1";
    }

}
