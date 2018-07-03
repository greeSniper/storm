package com.gaiaworks.service;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by 唐哲
 * 2018-02-13 10:15
 * 切割单词Bolt
 * 将书的每一行切割成单词数据
 * 将单词发射给下一个Bolt处理
 */
public class SplitBolt extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String line = input.getStringByField("line");
        long startTime = input.getLongByField("startTime");
        boolean flag = input.getBooleanByField("flag");

        String[] words = line.split("[\\s+\\[\\],\\.+;'\"]");

        for(String word : words) {
            if(!"".equals(word.trim())) {
                this.collector.emit(new Values(word, startTime, flag));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "startTime", "flag"));
    }

    public static void main(String[] args) {
        String book = "Though Whitman died thirteen 'years' ago, the time.";
        String[] split = book.split("[\\s+\\[\\],\\.+;'\"]");
        for(String word : split) {
            if(!"".equals(word.trim())) {
                System.out.println(word);
            }
        }
    }

}
