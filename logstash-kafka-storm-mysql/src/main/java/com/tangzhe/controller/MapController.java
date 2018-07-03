package com.tangzhe.controller;

import com.tangzhe.storm.MyBolt;
import com.tangzhe.storm.MySpout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.ModelAndView;

/**
 * Created by 唐哲
 * 2018-02-12 15:27
 */
@Controller
public class MapController {

    @Autowired
    private MySpout mySpout;
    @Autowired
    private MyBolt mybolt;

    @GetMapping("/map")
    public ModelAndView map() {
        return new ModelAndView("map");
    }

    @GetMapping
    @ResponseBody
    public String test() {
        return "test";
    }

}
