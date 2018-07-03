package com.gaiaworks.service;

import org.apache.storm.shade.org.apache.commons.io.FileUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created by 唐哲
 * 2018-02-13 10:07
 * 使用Storm完成词频统计功能
 * 数据源来自.txt文件
 * 将书的每一行发射给下一个Bolt处理
 */
public class DataSourceSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private long startTime;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        startTime = System.currentTimeMillis();

        //获取文件夹下的所有txt文件
        Collection<File> files = FileUtils.listFiles(new File("D:\\logs\\storm\\book"), new String[]{"txt"}, true);
        boolean flag = false;
        if(files!=null && files.size()>0) {
            flag = true;
        }
        for(File file : files) {
            try {
                //获取文件所有行
                List<String> lines = FileUtils.readLines(file);
                //遍历每个文件的每一行
                for(String line : lines) {
                    //发射出去
                    this.collector.emit(new Values(line, startTime, false));
                }

                //数据处理完之后，改名，否则一直重复执行
                FileUtils.moveFile(file, new File(file.getAbsolutePath() + System.currentTimeMillis()));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if(flag) {
            this.collector.emit(new Values("thisIsFlag", startTime, true));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line", "startTime", "flag"));
    }

}
