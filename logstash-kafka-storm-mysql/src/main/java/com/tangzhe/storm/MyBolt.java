package com.tangzhe.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.*;
import java.util.Map;

/**
 * Created by 唐哲
 * 2018-02-12 16:17
 */
public class MyBolt extends BaseRichBolt {

    private FileWriter fw;
    private boolean flag;
    private long startTime;
    private long endTime;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        try {
            fw = new FileWriter("D:\\IdeaProjects\\Gaiaworks\\logstash-kafka-storm-mysql\\result");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple input) {
        Integer num = input.getIntegerByField("num");
        Integer count = input.getIntegerByField("count");
        if(count == 1) {
            startTime = System.currentTimeMillis();
            try {
                fw.write("----------start----------\n" + num + "\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if(num!=-1 && num%2==0) {
            try {
                fw.write(num + "\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else if(num == -1) {
            try {
                if(!flag) {
                    flag = true;
                    endTime = System.currentTimeMillis();
                    fw.write("----------end cost: " + (endTime-startTime) + "ms----------");
                    fw.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

}
