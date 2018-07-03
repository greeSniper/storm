package com.gaiaworks.integration.jdbc;

import com.google.common.collect.Maps;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.topology.TopologyBuilder;

import java.util.Map;

/**
 * Created by 唐哲
 * 2018-02-09 10:38
 */
public class LocalWordCountJdbcStormTopology {

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("jdbcDataSourceSpout", new JdbcDataSourceSpout());
        builder.setBolt("jdbcTransferBolt", new JdbcTransferBolt()).shuffleGrouping("jdbcDataSourceSpout");
        builder.setBolt("jdbcCountBolt", new JdbcCountBolt()).shuffleGrouping("jdbcTransferBolt");

        Map hikariConfigMap = Maps.newHashMap();
        hikariConfigMap.put("dataSourceClassName", "com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
        hikariConfigMap.put("dataSource.url", "jdbc:mysql://localhost/test");
        hikariConfigMap.put("dataSource.user", "root");
        hikariConfigMap.put("dataSource.password", "123456");
        ConnectionProvider connectionProvider = new HikariCPConnectionProvider(hikariConfigMap);

        String tableName = "wc";
        SimpleJdbcMapper simpleJdbcMapper = new SimpleJdbcMapper(tableName, connectionProvider);

        JdbcInsertBolt userPersistanceBolt = new JdbcInsertBolt(connectionProvider, simpleJdbcMapper).withTableName(tableName).withQueryTimeoutSecs(30);

        builder.setBolt("jdbcInsertBolt", userPersistanceBolt).shuffleGrouping("jdbcCountBolt");

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("LocalWordCountJdbcStormTopology", new Config(), builder.createTopology());
    }

}
