package com.tangzhe.config;

import com.tangzhe.storm.MyBolt;
import com.tangzhe.storm.MySpout;
import com.tangzhe.storm.MyTopology;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Created by 唐哲
 * 2018-02-12 17:40
 */
@Configuration
public class StormConfig {

    @Bean
    public MySpout mySpout() {
        return new MySpout();
    }

    @Bean
    public MyBolt myBolt() {
        return new MyBolt();
    }

    @Bean
    public TopologyBuilder builder() {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(MySpout.class.getSimpleName(), mySpout());
        builder.setBolt(MyBolt.class.getSimpleName(), myBolt()).shuffleGrouping(MySpout.class.getSimpleName());

        return builder;
    }

    @Bean
    public LocalCluster cluster() {
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(MyTopology.class.getSimpleName(), new Config(), builder().createTopology());
        return cluster;
    }

}
