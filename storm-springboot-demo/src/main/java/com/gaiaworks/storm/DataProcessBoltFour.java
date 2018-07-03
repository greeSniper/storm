package com.gaiaworks.storm;

import com.gaiaworks.entity.Punch;
import com.gaiaworks.utils.DateUtils;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by 唐哲
 * 2018-02-27 15:51
 */
@Slf4j
public class DataProcessBoltFour extends BaseRichBolt {

    private OutputCollector collector;

    //用于存储打卡记录，Map<key, punch>
    private Map<DataProcessBoltFour.key, Punch> map = new HashMap<>();

    private long costTime = 0;

    public Map<String, Object> getComponentConfiguration() {
        //设置发送tickTuple的时间间隔
        Config conf = new Config();
        conf.put(conf.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 1);
        return conf;
    }

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        try {
            if (input.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) &&
                    input.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID)) {

                long start = System.currentTimeMillis();
                for(Map.Entry<DataProcessBoltFour.key, Punch> entry : map.entrySet()) {
                    DataProcessBoltFour.key k = entry.getKey();
                    Punch value = entry.getValue();
                    if(!"正常".equals(value.getMsg())) {
                        log.info("{}\t{}\t{}\t{}\t\tcount: {}\tcost: {}ms", value.getPersonId(), value.getName(),
                                DateUtils.getInstance().getString(value.getPunchTime()), value.getMsg(),
                                Math.round(Integer.parseInt(value.getPersonId())*3), value.getCostTime());
                    }
                }
                //清空map
                map.clear();
                costTime += System.currentTimeMillis() - start;
            } else {
                Punch punch = (Punch) input.getValueByField("punch");
                long startTime = input.getLongByField("startTime");
                long endTime = System.currentTimeMillis();
                punch.setCostTime(endTime - startTime - costTime);

                DataProcessBoltFour.key key = new DataProcessBoltFour.key(punch.getPersonId(), punch.getPunchFlag());
                Punch p = map.get(key);
                if(p == null) {
                    map.put(key, punch);
                } else {
                    if(!"正常".equals(p.getMsg()) && "正常".equals(punch.getMsg())) {
                        //map中打卡记录异常，punch中正常，则将map中打卡记录设置为正常
                        map.put(key, punch);
                    }
                }
            }

            this.collector.ack(input);
        } catch (Exception e) {
            this.collector.fail(input);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Data
    public static class key {
        private String personId;
        private Integer punchFlag;

        public key() {}
        public key(String personId, Integer punchFlag) {
            this.personId = personId;
            this.punchFlag = punchFlag;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            key key = (key) o;

            if (personId != null ? !personId.equals(key.personId) : key.personId != null) return false;
            return punchFlag != null ? punchFlag.equals(key.punchFlag) : key.punchFlag == null;
        }

        @Override
        public int hashCode() {
            int result = (personId != null ? personId.hashCode() : 0) + (punchFlag != null ? punchFlag.hashCode() : 0);
            return result;
        }
    }

}
