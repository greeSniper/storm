package com.gaiaworks.storm.task1;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Created by 唐哲
 * 2018-02-06 18:21
 */
@Component("myBoltOne")
public class MyBoltOne extends BaseRichBolt {
    
    private OutputCollector collector;
    
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        Integer num = input.getIntegerByField("num");
        if(num%2 == 0) {

        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
    
}
