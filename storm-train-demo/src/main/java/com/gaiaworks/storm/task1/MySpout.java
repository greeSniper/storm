package com.gaiaworks.storm.task1;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Random;

/**
 * Created by 唐哲
 * 2018-02-06 17:44
 *
 * 随机生成100K个整数
 */
@Component("mySpout")
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
        int num = random.nextInt(100);
        collector.emit(new Values(num));
        count ++;
        if(count == 10) {
            System.out.println("~~~~~~~~~~~~~~~~~");
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("num"));
    }

}
