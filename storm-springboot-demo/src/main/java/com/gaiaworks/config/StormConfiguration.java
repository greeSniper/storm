package com.gaiaworks.config;

import com.gaiaworks.storm.*;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.topology.TopologyBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.UUID;

/**
 * Created by 唐哲
 * 2018-02-27 15:31
 */
@Configuration
public class StormConfiguration {

    @Bean
    public SpoutConfig spoutConfig() {
        //Kafka使用的zk地址
        BrokerHosts hosts = new ZkHosts("localhost:2181");

        //Kafka存储数据的topic名称
        String topic = "storm_topic";

        //指定ZK中的一个根目录，存储的是KafkaSpout读取数据的位置信息(offset)
        String zkRoot = "/" + topic;

        String id = UUID.randomUUID().toString();

        SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, zkRoot, id);

        //设置读取偏移量的操作
        spoutConfig.startOffsetTime = kafka.api.OffsetRequest.EarliestTime();

        return spoutConfig;
    }

    @Bean
    public KafkaSpout kafkaSpout() {
        return new KafkaSpout(this.spoutConfig());
    }

    @Bean
    public DataProcessBoltOne dataProcessBoltOne() {
        return new DataProcessBoltOne();
    }

    @Bean
    public DataProcessBoltTwo dataProcessBoltTwo() {
        return new DataProcessBoltTwo();
    }

    @Bean
    public DataProcessBoltThree dataProcessBoltThree() {
        return new DataProcessBoltThree();
    }

    @Bean
    public DataProcessBoltFour dataProcessBoltFour() {
        return new DataProcessBoltFour();
    }

    @Bean
    public TopologyBuilder builder() {
        TopologyBuilder builder = new TopologyBuilder();

        String SPOUT_ID = KafkaSpout.class.getSimpleName();
        builder.setSpout(SPOUT_ID, kafkaSpout());

        String BOLT_ONE_ID = DataProcessBoltOne.class.getSimpleName();
        builder.setBolt(BOLT_ONE_ID, dataProcessBoltOne())
                .shuffleGrouping(SPOUT_ID);

        String BOLT_TWO_ID = DataProcessBoltTwo.class.getSimpleName();
        builder.setBolt(BOLT_TWO_ID, dataProcessBoltTwo())
                .shuffleGrouping(BOLT_ONE_ID);

        String BOLT_THREE_ID = DataProcessBoltThree.class.getSimpleName();
        builder.setBolt(BOLT_THREE_ID, dataProcessBoltThree())
                .shuffleGrouping(BOLT_TWO_ID);

        String BOLT_FOUR_ID = DataProcessBoltFour.class.getSimpleName();
        builder.setBolt(BOLT_FOUR_ID, dataProcessBoltFour())
                .shuffleGrouping(BOLT_THREE_ID);

//        String TEST_BOLT_ID = TestBolt.class.getSimpleName();
//        builder.setBolt(TEST_BOLT_ID, new TestBolt()).shuffleGrouping(SPOUT_ID);

        return builder;
    }

    @Bean
    public LocalCluster cluster() {
        LocalCluster cluster = new LocalCluster();
        Config config = new Config();
        //config.setNumWorkers(2);
        cluster.submitTopology(StormConfiguration.class.getSimpleName(), config, builder().createTopology());
        return cluster;
    }

}
