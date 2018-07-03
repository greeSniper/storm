package com.gaiaworks.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by 唐哲
 * 2018-02-13 10:24
 * 词频汇总Bolt
 * 将结果写入日志文件中
 */
@Slf4j
public class CountBolt extends BaseRichBolt {

    private Map<String, Integer> map = new HashMap<>();

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {

    }

    @Override
    public void execute(Tuple input) {
        //获取每一个单词并进行汇总
        String word = input.getStringByField("word");
        long startTime = input.getLongByField("startTime");
        boolean flag = input.getBooleanByField("flag");

        Integer count = map.get(word);
        if(count == null) {
            count = 0;
        }
        count ++;

        map.put(word, count);

        //输出
        if(flag) {
            for(Map.Entry<String, Integer> entry : map.entrySet()) {
                log.info(entry.getKey() + "->" + entry.getValue());
            }
            log.info("---------------cost: {}ms", System.currentTimeMillis()-startTime);

            map.clear();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

}
