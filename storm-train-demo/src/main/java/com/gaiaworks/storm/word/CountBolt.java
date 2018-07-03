package com.gaiaworks.storm.word;

import lombok.extern.slf4j.Slf4j;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by 唐哲
 * 2018-02-06 17:12
 * 
 * 词频汇总Bolt
 */
@Component
@Slf4j
public class CountBolt extends BaseRichBolt {

    private Map<String, Integer> map = new HashMap<>();

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        
    }

    /**
     * 业务逻辑：
     * 1）获取每个单词
     * 2）对所有单词进行汇总
     * 3）输出
     */
    @Override
    public void execute(Tuple input) {
        //获取每一个单词并进行汇总
        String word = input.getStringByField("word");
        Integer count = map.get(word);
        if(count == null) {
            count = 0;
        }
        count ++;

        map.put(word, count);

        //输出
        log.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        for(Map.Entry<String, Integer> entry : map.entrySet()) {
            log.info(entry.getKey() + "->" + entry.getValue());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
    
}
