package com.tangzhe.kafka;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created by 唐哲
 * 2018-02-12 9:58
 * 接收Kafka的数据进行处理的BOLT
 */
public class LogProcessBolt extends BaseRichBolt {

    private OutputCollector collector;

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        try {
            byte[] binaryByField = input.getBinaryByField("bytes");
            String value = new String(binaryByField);

            /**
             * 13666666666	116.544079,40.417555	[2018-02-12 11:40:15]
             * 解析出来的日志信息
             */
            String[] split = value.split("\t");
            String phone = split[0];
            String[] temp = split[1].split(",");
            String longitude = temp[0];
            String latitude = temp[1];
            long time = DateUtils.getInstance().getTime(split[2]);

            System.out.println(phone + "," + longitude + "," + latitude + "," + time);

            this.collector.ack(input);
        } catch (Exception e) {
            this.collector.fail(input);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

}
