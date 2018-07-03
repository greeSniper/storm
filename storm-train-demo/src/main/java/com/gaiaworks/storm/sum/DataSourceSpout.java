package com.gaiaworks.storm.sum;

import lombok.extern.slf4j.Slf4j;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * 使用Storm实现积累求和的操作
 * Created by 唐哲
 * 2018-02-06 15:33
 *
 * Spout需要继承BaseRichSpout
 * 数据源需要产生数据并发射
 */
@Component("dataSourceSpout1")
@Slf4j
public class DataSourceSpout extends BaseRichSpout {

    private SpoutOutputCollector spoutOutputCollector;
    private int number = 0;

    /**
     * 初始化方法，只会被调用一次
     * @param conf  配置参数
     * @param topologyContext  上下文
     * @param spoutOutputCollector 数据发射器
     */
    @Override
    public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
    }

    /**
     * 会产生数据，在生产上肯定是从消息队列中获取数据
     * 这个方法是一个死循环，会一直不停的执行
     */
    @Override
    public void nextTuple() {
        /**
         * emit方法有两个参数：
         *  1） 数据
         *  2） 数据的唯一编号 msgId  如果是数据库，msgId就可以采用表中的主键
         */
        this.spoutOutputCollector.emit(new Values(++number), "number");
        log.info("Spout: " + number);
        try {
            //防止数据产生太快
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 声明输出字段
     * @param outputFieldsDeclarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("num"));
    }

    @Override
    public void ack(Object msgId) {
        System.out.println("ack invoked..." + msgId);
    }

    @Override
    public void fail(Object msgId) {
        System.out.println("fail invoked..." + msgId);

        //TODO... 此处对失败的数据进行重发或者保存下来
        //this.collector.emit(tuple)
        //this.dao.saveMsg(msgId)
    }

}
