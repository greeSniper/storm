package com.gaiaworks.storm;

import lombok.extern.slf4j.Slf4j;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created by 唐哲
 * 2018-02-28 9:46
 */
@Slf4j
public class TestBolt extends BaseRichBolt {

    private boolean flag = false;
    private long startTime;
    private long cost = 0;

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {

    }

    public void execute(Tuple input) {
        if(!flag) {
            startTime = System.currentTimeMillis();
            flag = true;
        }

        byte[] binaryByField = input.getBinaryByField("bytes");
        String record = new String(binaryByField);

        long costTime = System.currentTimeMillis() - startTime - cost;
        long start = System.currentTimeMillis();
        log.info("{}\tcost:{}ms", record, costTime);
        cost += System.currentTimeMillis() - start;
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

}
