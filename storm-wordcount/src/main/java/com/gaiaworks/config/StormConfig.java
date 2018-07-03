package com.gaiaworks.config;

import com.gaiaworks.service.CountBolt;
import com.gaiaworks.service.DataSourceSpout;
import com.gaiaworks.service.SplitBolt;
import com.gaiaworks.service.WordCountTopology;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Created by 唐哲
 * 2018-02-13 11:31
 */
@Configuration
public class StormConfig {

    @Bean
    public DataSourceSpout dataSourceSpout() {
        return new DataSourceSpout();
    }

    @Bean
    public SplitBolt splitBolt() {
        return new SplitBolt();
    }

    @Bean
    public CountBolt countBolt() {
        return new CountBolt();
    }

    @Bean
    public TopologyBuilder builder() {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(DataSourceSpout.class.getSimpleName(), dataSourceSpout());
        builder.setBolt(SplitBolt.class.getSimpleName(), splitBolt()).shuffleGrouping(DataSourceSpout.class.getSimpleName());
        builder.setBolt(CountBolt.class.getSimpleName(), countBolt()).shuffleGrouping(SplitBolt.class.getSimpleName());
        return builder;
    }

    @Bean
    public LocalCluster cluster() {
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(WordCountTopology.class.getSimpleName(), new Config(), builder().createTopology());
        return cluster;
    }

}
