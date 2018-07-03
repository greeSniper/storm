package com.tangzhe.storm;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

/**
 * Created by 唐哲
 * 2018-02-12 16:00
 */
public class MySpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private Random random = new Random();
    private Integer count = 0;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        //this.collector.emit(new Values(random.nextInt(100), ++count), UUID.randomUUID().toString());
        if(count < 100000) {
            this.collector.emit(new Values(random.nextInt(100), ++count));
        } else {
            this.collector.emit(new Values(-1, count));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("num", "count"));
    }

    @Override
    public void ack(Object msgId) {}

    @Override
    public void fail(Object msgId) {
        //this.collector.emit(new Values(random.nextInt(100), ++count), msgId);
    }

}
