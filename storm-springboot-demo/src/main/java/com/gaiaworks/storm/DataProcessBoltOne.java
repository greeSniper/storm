package com.gaiaworks.storm;

import com.gaiaworks.entity.Punch;
import com.gaiaworks.utils.DateUtils;
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
 * 2018-02-27 15:32
 */
public class DataProcessBoltOne extends BaseRichBolt {

    private OutputCollector collector;

    private long startTime;
    private boolean flag;

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        if(!flag) {
            startTime = System.currentTimeMillis();
            flag = true;
        }

        byte[] binaryByField = input.getBinaryByField("bytes");
        String record = new String(binaryByField);

        /**
         *  1,祝阳曦,2018.02.14 08:12:34,2018.02.14 08:30:00, , ,2018.02.14 17:30:00
            1,祝阳曦,2018.02.14 18:09:21,2018.02.14 08:30:00, , ,2018.02.14 17:30:00
            2,查立辉,2018.02.14 07:58:54,2018.02.14 08:30:00,2018.02.14 12:00:00,2018.02.14 13:00:00,2018.02.14 17:30:00
            2,查立辉,2018.02.14 11:58:04,2018.02.14 08:30:00,2018.02.14 12:00:00,2018.02.14 13:00:00,2018.02.14 17:30:00
            2,查立辉,2018.02.14 13:02:42,2018.02.14 08:30:00,2018.02.14 12:00:00,2018.02.14 13:00:00,2018.02.14 17:30:00
            2,查立辉,2018.02.14 17:35:45,2018.02.14 08:30:00,2018.02.14 12:00:00,2018.02.14 13:00:00,2018.02.14 17:30:00
         */
        String[] split = record.split(",");
        String personId = split[0];
        String name = split[1];
        try {
            Long punchTime = DateUtils.getInstance().getTime(split[2]);
            Long workStartTime = DateUtils.getInstance().getTime(split[3]);
            Long noonStartTime = null;
            if(!" ".equals(split[4])) {
                noonStartTime = DateUtils.getInstance().getTime(split[4]);
            }
            Long noonEndTime = null;
            if(!" ".equals(split[5])) {
                noonEndTime = DateUtils.getInstance().getTime(split[5]);
            }
            Long workEndTime = DateUtils.getInstance().getTime(split[6]);

            Punch punch = new Punch(personId, name, punchTime,
                    workStartTime, noonStartTime, noonEndTime, workEndTime);

            collector.emit(new Values(punch, startTime));

            //this.collector.ack(input);
        } catch (Exception e) {
            //this.collector.fail(input);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("punch", "startTime"));
    }

}
