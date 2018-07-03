package com.gaiaworks.integration.hdfs;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by 唐哲
 * 2018-02-09 10:26
 */
public class HdfsCountBolt extends BaseRichBolt {

    private OutputCollector collector;
    private Map<String, Integer> map = new HashMap<>();

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String word = input.getStringByField("word2");
        Integer count = map.get(word);
        if(count == null) {
            count = 0;
        }
        map.put(word, ++count);

        this.collector.emit(new Values(word, map.get(word)));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }

}
