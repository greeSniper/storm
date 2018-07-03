package com.gaiaworks.integration.redis;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.redis.bolt.RedisStoreBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Created by 唐哲
 * 2018-02-09 10:38
 */
public class LocalWordCountRedisStormTopology {

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("dataSourceSpout", new DataSourceSpout());
        builder.setBolt("transferBolt", new TransferBolt()).shuffleGrouping("dataSourceSpout");
        builder.setBolt("countBolt", new CountBolt()).shuffleGrouping("transferBolt");

        JedisPoolConfig poolConfig = new JedisPoolConfig.Builder()
                .setHost("192.168.93.128").setPort(6379).build();
        RedisStoreMapper storeMapper = new WordCountStoreMapper();
        RedisStoreBolt storeBolt = new RedisStoreBolt(poolConfig, storeMapper);

        builder.setBolt("redisStoreBolt", storeBolt).shuffleGrouping("countBolt");

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("LocalWordCountRedisStormTopology", new Config(), builder.createTopology());
    }

}
