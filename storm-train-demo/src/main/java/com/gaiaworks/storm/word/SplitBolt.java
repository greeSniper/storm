package com.gaiaworks.storm.word;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Created by 唐哲
 * 2018-02-06 17:07
 * 
 * 对数据进行切割
 */
@Component
public class SplitBolt extends BaseRichBolt {
    
    private OutputCollector collector;
    
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    /**
     * 业务逻辑：
     *   line： 对line进行分割，按照逗号
     */
    @Override
    public void execute(Tuple input) {
        String line = input.getStringByField("line");
        String[] words = line.split(",");

        for(String word : words) {
            this.collector.emit(new Values(word));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
    
}
