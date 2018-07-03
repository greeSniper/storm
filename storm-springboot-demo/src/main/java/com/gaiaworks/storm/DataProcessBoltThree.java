package com.gaiaworks.storm;

import com.gaiaworks.entity.Punch;
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
 * 2018-02-27 15:48
 */
public class DataProcessBoltThree extends BaseRichBolt {

    private OutputCollector collector;

    private long startTime;

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        try {
            Punch punch = (Punch) input.getValueByField("punch");
            startTime = input.getLongByField("startTime");

            if(punch.getPunchFlag() == 0) {
                //上班打卡
                if(punch.getPunchTime() > punch.getWorkStartTime()) {
                    //上班迟到
                    punch.setMsg("上班迟到");
                }
            } else if(punch.getPunchFlag() == 1) {
                //午休前打卡
                if(punch.getPunchTime() < punch.getNoonStartTime()) {
                    //午休早退
                    punch.setMsg("午休早退");
                }
            } else if(punch.getPunchFlag() == 2) {
                //午休后打卡
                if(punch.getPunchTime() > punch.getNoonEndTime()) {
                    //午休迟到
                    punch.setMsg("午休迟到");
                }
            } else if(punch.getPunchFlag() == 3) {
                //下班打卡
                if(punch.getPunchTime() < punch.getWorkEndTime()) {
                    //下班早退
                    punch.setMsg("下班早退");
                }
            }

            //异常正常的打卡记录都要发送给下一个bolt
            this.collector.emit(new Values(punch, startTime));

            this.collector.ack(input);
        } catch (Exception e) {
            this.collector.fail(input);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("punch", "startTime"));
    }

}
