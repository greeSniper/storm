package com.gaiaworks.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Created by 唐哲
 * 2018-02-27 15:09
 */
@RestController
public class ProducerController {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @RequestMapping("/send/{msg}")
    public String send(@PathVariable String msg) {
        kafkaTemplate.send("storm_topic", msg);
        return "send...";
    }

}
