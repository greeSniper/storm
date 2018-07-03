package com.gaiaworks.job;

import com.gaiaworks.utils.GenerateData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Created by 唐哲
 * 2018-02-27 13:46
 */
@Component
@EnableScheduling
public class DataJob {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    /**
     * 每秒发送一条记录
     */
    @Scheduled(fixedRate = 1000)
    public void weatherDataSyncJob() {
        for(int i=0; i<100; i++) {
            kafkaTemplate.send("storm_topic", GenerateData.generateOne(i));
        }
    }

}
