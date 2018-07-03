package com.gaiaworks.integration.redis;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Created by 唐哲
 * 2018-02-09 10:14
 */
public class DataSourceSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private List<String> words = Arrays.asList("abel", "ben", "chris", "demo");

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        Random random = new Random();
        int index = random.nextInt(words.size());
        this.collector.emit(new Values(words.get(index)));

        System.out.println("emit: " + words.get(index));

        Utils.sleep(1000);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word1"));
    }

}
