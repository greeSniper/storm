package com.gaiaworks.integration.hdfs;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Created by 唐哲
 * 2018-02-09 10:38
 */
public class LocalWordCountHdfsStormTopology {

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("hdfsDataSourceSpout", new HdfsDataSourceSpout());
        builder.setBolt("hdfsTransferBolt", new HdfsTransferBolt()).shuffleGrouping("hdfsDataSourceSpout");
        builder.setBolt("hdfsCountBolt", new HdfsCountBolt()).shuffleGrouping("hdfsTransferBolt");

        RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter("|");

        SyncPolicy syncPolicy = new CountSyncPolicy(100);

        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, FileSizeRotationPolicy.Units.KB);

        FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath("/foo/");

        HdfsBolt bolt = new HdfsBolt()
                .withFsUrl("hdfs://192.168.93.128:8020")
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy);

        builder.setBolt("hdfsBolt", bolt).shuffleGrouping("hdfsCountBolt");

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("LocalWordCountHdfsStormTopology", new Config(), builder.createTopology());
    }

}
