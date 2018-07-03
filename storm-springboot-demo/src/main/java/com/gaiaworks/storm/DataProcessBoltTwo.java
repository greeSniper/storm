package com.gaiaworks.storm;

import com.gaiaworks.entity.Punch;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by 唐哲
 * 2018-02-27 15:42
 */
public class DataProcessBoltTwo extends BaseRichBolt {

    private OutputCollector collector;

    private long startTime;

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        try {
            Punch punch = (Punch) input.getValueByField("punch");
            startTime = input.getLongByField("startTime");
            long punchTime = punch.getPunchTime();

            long workStartDiff = Math.abs(punchTime - punch.getWorkStartTime());
            long workEndDiff = Math.abs(punchTime - punch.getWorkEndTime());
            if(punch.getNoonStartTime()==null || punch.getNoonEndTime()==null) {
                //1.只有上下班打卡
                if(workStartDiff < workEndDiff) {
                    //打卡时间更接近上班时间
                    this.nearWorkStart(punch, punchTime, workStartDiff);
                } else {
                    //打卡时间更接近下班时间
                    this.nearWorkEnd(punch, punchTime, workEndDiff);
                }
            } else {
                //2.有上下班打卡也有午休打卡
                long noonStartDiff = Math.abs(punchTime - punch.getNoonStartTime());
                long noonEndDiff = Math.abs(punchTime - punch.getNoonEndTime());

                //求4个差值中的最小值，即打卡时间最接近的标准时间
                long minDiff = this.getMinDiff(workStartDiff, noonStartDiff, noonEndDiff, workEndDiff);
                if(workStartDiff == minDiff) {
                    //打卡时间更接近上班时间
                    this.nearWorkStart(punch, punchTime, workStartDiff);
                } else if(noonStartDiff == minDiff) {
                    //打卡时间更接近上午休前打卡
                    this.nearNoonStart(punch, punchTime, noonStartDiff);
                } else if(noonEndDiff == minDiff) {
                    //打卡时间更接近上午休后打卡
                    this.nearNoonEnd(punch, punchTime, noonEndDiff);
                } else {
                    //打卡时间更接近下班时间
                    this.nearWorkEnd(punch, punchTime, workEndDiff);
                }
            }

            this.collector.ack(input);
        } catch (Exception e) {
            this.collector.fail(input);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("punch", "startTime"));
    }

    /**
     * 打卡时间更接近上班时间
     */
    private void nearWorkStart(Punch punch, long punchTime, long workStartDiff) {
        //判断是昨天下班打卡(加班)还是早上上班打卡
        long yestWorkEndTime = punch.getWorkEndTime()-24*60*60*1000;
        //打卡时间与昨天下班标准时间的差值
        long yestDiff = Math.abs(punchTime-yestWorkEndTime);
        if(yestDiff <= workStartDiff) {
            //昨天下班打卡，暂时不进行处理
        } else {
            //今天上班打卡，将punch的punchFlag设置为0
            punch.setPunchFlag(0);
            //发射
            this.collector.emit(new Values(punch, startTime));
        }
    }

    /**
     * 打卡时间更接近下班时间
     */
    private void nearWorkEnd(Punch punch, long punchTime, long workEndDiff) {
        //判断是今天下班打卡还是明天上班打卡
        long tomWorkStartTime = punch.getWorkStartTime()+24*60*60*1000;
        //打卡时间与明天上班标准时间的差值
        long tomDiff = Math.abs(punchTime-tomWorkStartTime);
        if(tomDiff <= workEndDiff) {
            //明天上班打卡，暂时不进行处理
        } else {
            //今天下班打卡，将punch的punchFlag设置为3
            punch.setPunchFlag(3);
            //发射
            this.collector.emit(new Values(punch, startTime));
        }
    }

    /**
     * 打卡时间更接近上午休前打卡
     */
    private void nearNoonStart(Punch punch, long punchTime, long noonStartDiff) {
        //将punch的punchFlag设置为1
        punch.setPunchFlag(1);
        //发射
        this.collector.emit(new Values(punch, startTime));
    }

    /**
     * 打卡时间更接近上午休后打卡
     */
    private void nearNoonEnd(Punch punch, long punchTime, long noonEndDiff) {
        //将punch的punchFlag设置为2
        punch.setPunchFlag(2);
        //发射
        this.collector.emit(new Values(punch, startTime));
    }

    /**
     * 求4个差值中的最小值
     */
    private long getMinDiff(long workStartDiff, long noonStartDiff, long noonEndDiff, long workEndDiff) {
        List<Long> diffList = new ArrayList<>(4);
        diffList.add(workStartDiff);
        diffList.add(noonStartDiff);
        diffList.add(noonEndDiff);
        diffList.add(workEndDiff);

        long minDiff = workStartDiff;
        for(long diff : diffList) {
            if(diff < minDiff) {
                minDiff = diff;
            }
        }

        return minDiff;
    }

}
