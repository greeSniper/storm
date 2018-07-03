package com.gaiaworks.storm.sum;

import lombok.extern.slf4j.Slf4j;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * 数据的累积求和Bolt：接收数据并处理
 * Created by 唐哲
 * 2018-02-06 16:16
 */
@Component
@Slf4j
public class SumBolt extends BaseRichBolt {

    private OutputCollector outputCollector;

    private int sum = 0;

    /**
     * 初始化方法，会被执行一次
     * @param stormConf
     * @param topologyContext
     * @param outputCollector
     */
    @Override
    public void prepare(Map stormConf, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    /**
     * 其实也是一个死循环，职责：获取Spout发送过来的数据
     * @param input
     */
    @Override
    public void execute(Tuple input) {
        Integer num = input.getIntegerByField("num");
        sum += num;
        log.info("Bolt: " + sum);

//        try {
//            //TODO... 你的业务逻辑
//            this.outputCollector.ack(input);
//        } catch (Exception e) {
//            this.outputCollector.fail(input);
//        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

}
